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

#include "phrase_edge_query.h"

#include <climits>
#include <fstream>
#include <functional>
#include <string>
#include <string_view>

#include "CLucene/config/repl_wchar.h"
#include "CLucene/util/stringUtil.h"
#include "common/logging.h"

namespace doris::segment_v2 {

PhraseEdgeQuery::PhraseEdgeQuery(const std::shared_ptr<lucene::search::IndexSearcher>& searcher,
                                 const TQueryOptions& query_options, const io::IOContext* io_ctx)
        : _searcher(searcher),
          _io_ctx(io_ctx),
          _query(std::make_unique<CL_NS(search)::MultiPhraseQuery>()),
          _max_expansions(query_options.inverted_index_max_expansions) {}

void PhraseEdgeQuery::add(const InvertedIndexQueryInfo& query_info) {
    if (query_info.term_infos.empty()) {
        throw Exception(ErrorCode::INVALID_ARGUMENT, "term_infos cannot be empty");
    }
    _field_name = query_info.field_name;
    _term_infos = query_info.term_infos;
}

void PhraseEdgeQuery::search(roaring::Roaring& roaring) {
    if (_term_infos.size() == 1) {
        search_one_term(roaring);
    } else {
        search_multi_term(roaring);
    }
}

void PhraseEdgeQuery::search_one_term(roaring::Roaring& roaring) {
    bool first = true;
    std::wstring sub_term = StringUtil::string_to_wstring(_term_infos[0].get_single_term());
    find_words([this, &first, &sub_term, &roaring](Term* term) {
        std::wstring_view ws_term(term->text(), term->textLength());
        if (ws_term.find(sub_term) == std::wstring::npos) {
            return;
        }

        DocRange doc_range;
        TermDocs* term_doc = _searcher->getReader()->termDocs(term);
        roaring::Roaring result;
        while (term_doc->readRange(&doc_range)) {
            if (doc_range.type_ == DocRangeType::kMany) {
                result.addMany(doc_range.doc_many_size_, doc_range.doc_many->data());
            } else {
                result.addRange(doc_range.doc_range.first, doc_range.doc_range.second);
            }
        }
        _CLDELETE(term_doc);

        if (!first) {
            roaring.swap(result);
            first = false;
        } else {
            roaring |= result;
        }
    });
}

void PhraseEdgeQuery::search_multi_term(roaring::Roaring& roaring) {
    std::wstring suffix_term = StringUtil::string_to_wstring(_term_infos[0].get_single_term());
    std::wstring prefix_term = StringUtil::string_to_wstring(_term_infos.back().get_single_term());

    std::vector<CL_NS(index)::Term*> suffix_terms;
    std::vector<CL_NS(index)::Term*> prefix_terms;

    find_words([this, &suffix_term, &suffix_terms, &prefix_term, &prefix_terms](Term* term) {
        std::wstring_view ws_term(term->text(), term->textLength());

        if (_max_expansions == 0 || suffix_terms.size() < _max_expansions) {
            if (ws_term.ends_with(suffix_term)) {
                suffix_terms.push_back(_CL_POINTER(term));
            }
        }

        if (_max_expansions == 0 || prefix_terms.size() < _max_expansions) {
            if (ws_term.starts_with(prefix_term)) {
                prefix_terms.push_back(_CL_POINTER(term));
            }
        }
    });

    for (size_t i = 0; i < _term_infos.size(); i++) {
        if (i == 0) {
            handle_terms(_field_name, suffix_term, suffix_terms);
        } else if (i == _term_infos.size() - 1) {
            handle_terms(_field_name, prefix_term, prefix_terms);
        } else {
            std::wstring ws_term = StringUtil::string_to_wstring(_term_infos[i].get_single_term());
            add_default_term(_field_name, ws_term);
        }
    }

    _searcher->_search(_query.get(), [&roaring](const int32_t docid, const float_t /*score*/) {
        roaring.add(docid);
    });
}

void PhraseEdgeQuery::add_default_term(const std::wstring& field_name,
                                       const std::wstring& ws_term) {
    Term* t = _CLNEW Term(field_name.c_str(), ws_term.c_str());
    _query->add(t);
    _CLLDECDELETE(t);
}

void PhraseEdgeQuery::handle_terms(const std::wstring& field_name, const std::wstring& ws_term,
                                   std::vector<CL_NS(index)::Term*>& checked_terms) {
    if (checked_terms.empty()) {
        add_default_term(field_name, ws_term);
    } else {
        _query->add(checked_terms);
        for (const auto& t : checked_terms) {
            _CLLDECDELETE(t);
        }
    }
};

void PhraseEdgeQuery::find_words(const std::function<void(Term*)>& cb) {
    Term* term = nullptr;
    TermEnum* enumerator = nullptr;
    try {
        enumerator = _searcher->getReader()->terms(nullptr, _io_ctx);
        while (enumerator->next()) {
            term = enumerator->term();
            cb(term);
            _CLDECDELETE(term);
        }
    }
    _CLFINALLY({
        _CLDECDELETE(term);
        enumerator->close();
        _CLDELETE(enumerator);
    })
}

} // namespace doris::segment_v2