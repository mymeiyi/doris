// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

#include "olap/primary_key_index.h"

#include <gen_cpp/segment_v2.pb.h>

#include <utility>

#include "common/compiler_util.h" // IWYU pragma: keep
#include "common/config.h"
#include "io/fs/file_writer.h"
#include "olap/olap_common.h"
#include "olap/rowset/segment_v2/bloom_filter_index_reader.h"
#include "olap/rowset/segment_v2/bloom_filter_index_writer.h"
#include "olap/rowset/segment_v2/encoding_info.h"
#include "olap/types.h"
#include "vec/data_types/data_type.h"
#include "vec/data_types/data_type_factory.hpp"

namespace doris {

Status PrimaryKeyIndexBuilder::init() {
    // TODO(liaoxin) using the column type directly if there's only one column in unique key columns
    const auto* type_info = get_scalar_type_info<FieldType::OLAP_FIELD_TYPE_VARCHAR>();
    segment_v2::IndexedColumnWriterOptions options;
    options.write_ordinal_index = true;
    options.write_value_index = true;
    options.data_page_size = config::primary_key_data_page_size;
    options.encoding = segment_v2::EncodingInfo::get_default_encoding(type_info, true);
    options.compression = segment_v2::ZSTD;
    _primary_key_index_builder.reset(
            new segment_v2::IndexedColumnWriter(options, type_info, _file_writer));
    RETURN_IF_ERROR(_primary_key_index_builder->init());

    auto opt = segment_v2::BloomFilterOptions();
    opt.fpp = 0.01;
    _bloom_filter_index_builder.reset(
            new segment_v2::PrimaryKeyBloomFilterIndexWriterImpl(opt, type_info));
    return Status::OK();
}

Status PrimaryKeyIndexBuilder::add_item(const Slice& key) {
    RETURN_IF_ERROR(_primary_key_index_builder->add(&key));
    Slice key_without_seq = Slice(key.get_data(), key.get_size() - _seq_col_length);
    _bloom_filter_index_builder->add_values(&key_without_seq, 1);
    // the key is already sorted, so the first key is min_key, and
    // the last key is max_key.
    if (UNLIKELY(_num_rows == 0)) {
        _min_key.append(key.get_data(), key.get_size());
    }
    _max_key.clear();
    _max_key.append(key.get_data(), key.get_size());
    _num_rows++;
    _size += key.get_size();
    return Status::OK();
}

Status PrimaryKeyIndexBuilder::finalize(segment_v2::PrimaryKeyIndexMetaPB* meta) {
    // finish primary key index
    RETURN_IF_ERROR(_primary_key_index_builder->finish(meta->mutable_primary_key_index()));
    _disk_size += _primary_key_index_builder->disk_size();

    // set min_max key, the sequence column should be removed
    meta->set_min_key(min_key().to_string());
    meta->set_max_key(max_key().to_string());

    // finish bloom filter index
    RETURN_IF_ERROR(_bloom_filter_index_builder->flush());
    uint64_t start_size = _file_writer->bytes_appended();
    RETURN_IF_ERROR(
            _bloom_filter_index_builder->finish(_file_writer, meta->mutable_bloom_filter_index()));
    _disk_size += _file_writer->bytes_appended() - start_size;
    _primary_key_index_builder.reset(nullptr);
    _bloom_filter_index_builder.reset(nullptr);
    return Status::OK();
}

Status PrimaryKeyIndexReader::parse_index(io::FileReaderSPtr file_reader,
                                          const segment_v2::PrimaryKeyIndexMetaPB& meta) {
    // parse primary key index
    _index_reader.reset(new segment_v2::IndexedColumnReader(file_reader, meta.primary_key_index()));
    _index_reader->set_is_pk_index(true);
    RETURN_IF_ERROR(_index_reader->load(!config::disable_pk_storage_page_cache, false));

    _index_parsed = true;
    return Status::OK();
}

Status PrimaryKeyIndexReader::parse_bf(io::FileReaderSPtr file_reader,
                                       const segment_v2::PrimaryKeyIndexMetaPB& meta) {
    // parse bloom filter
    segment_v2::ColumnIndexMetaPB column_index_meta = meta.bloom_filter_index();
    segment_v2::BloomFilterIndexReader bf_index_reader(std::move(file_reader),
                                                       column_index_meta.bloom_filter_index());
    RETURN_IF_ERROR(bf_index_reader.load(!config::disable_pk_storage_page_cache, false));
    std::unique_ptr<segment_v2::BloomFilterIndexIterator> bf_iter;
    RETURN_IF_ERROR(bf_index_reader.new_iterator(&bf_iter));
    RETURN_IF_ERROR(bf_iter->read_bloom_filter(0, &_bf));
    _bf_parsed = true;

    return Status::OK();
}

Status PrimaryKeyIndexReader::seek_at_or_after(Slice& key_without_seq, bool exact_match,
                                               Slice& sought_key_without_seq,
                                               Slice& sought_key_sequence_id,
                                               uint32_t& sout_key_row_id) const {
    if (!check_present(key_without_seq)) {
        return Status::Error<ErrorCode::KEY_NOT_FOUND>("Can't find key in the segment");
    }
    std::unique_ptr<segment_v2::IndexedColumnIterator> index_iterator;
    RETURN_IF_ERROR(new_iterator(&index_iterator));
    auto st = index_iterator->seek_at_or_after(&key_without_seq, &exact_match);
    if (!st.ok() && !st.is<ErrorCode::ENTRY_NOT_FOUND>()) {
        return st;
    }
    if (st.is<ErrorCode::ENTRY_NOT_FOUND>() || (_seq_col_length == 0 && !exact_match)) {
        return Status::Error<ErrorCode::KEY_NOT_FOUND>("Can't find key in the segment");
    }
    sout_key_row_id = index_iterator->get_current_ordinal();
    if (_seq_col_length > 0) {
        size_t num_to_read = 1;
        auto index_type =
                vectorized::DataTypeFactory::instance().create_data_type(type_info()->type(), 1, 0);
        auto index_column = index_type->create_column();
        size_t num_read = num_to_read;
        RETURN_IF_ERROR(index_iterator->next_batch(&num_read, index_column));
        DCHECK(num_to_read == num_read);

        Slice sought_key =
                Slice(index_column->get_data_at(0).data, index_column->get_data_at(0).size);
        sought_key_without_seq =
                Slice(sought_key.get_data(), sought_key.get_size() - _seq_col_length);
        // compare key
        if (key_without_seq.compare(sought_key_without_seq) != 0) {
            return Status::Error<ErrorCode::KEY_NOT_FOUND>("Can't find key in the segment");
        }
        sought_key_sequence_id = Slice(
                sought_key.get_data() + sought_key_without_seq.get_size() + 1, _seq_col_length - 1);
    }
    return Status::OK();
}

} // namespace doris
