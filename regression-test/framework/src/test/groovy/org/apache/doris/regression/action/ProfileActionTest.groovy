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

package org.apache.doris.regression.action

import org.junit.jupiter.api.Test

import static org.junit.jupiter.api.Assertions.assertEquals

class ProfileActionTest {
    @Test
    void loadProfileIsFoundBehindOtherMatchingProfiles() {
        def action = new ProfileAction(null) {
            @Override
            List getProfileList() {
                ['pending', 'command', 'expired', 'load'].collect { id ->
                    ['Sql Statement': 'LOAD LABEL test_label', 'Profile ID': id,
                     'Profile Completion State': id == 'pending' ? 'INCOMPLETE' : 'COMPLETE']
                }
            }

            @Override
            String getProfile(String id) {
                if (id == 'expired') {
                    throw new IllegalStateException('profile expired')
                }
                return 'Profile Completion State: COMPLETE\n' + (id == 'load' ? 'DeltaWriterV2' : 'command')
            }
        }
        assertEquals('Profile Completion State: COMPLETE\nDeltaWriterV2',
                action.getProfileBySql('test_label', ['DeltaWriterV2'], 1000, 1))
    }
}
