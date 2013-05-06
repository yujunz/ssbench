# Copyright (c) 2012-2013 SwiftStack, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
# implied.
# See the License for the specific language governing permissions and
# limitations under the License.

_version = (0, 2, 14)
version = '.'.join(map(str, _version))

# Constant names (which happen to need to match methods called
# handle_<OPERATION> in worker.py)
CREATE_OBJECT = 'upload_object'
READ_OBJECT = 'get_object'
UPDATE_OBJECT = 'update_object'
DELETE_OBJECT = 'delete_object'
