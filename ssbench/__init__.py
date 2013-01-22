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


DEFAULT_TUBE = 'default'
STATS_TUBE = 'stats'
WORK_TUBE = 'work'

# NB: lower number --> higher priority (0 is highest, 2**32 - 1 is lowest)
# And 2**31 is the default
PRIORITY_SETUP = 5000
PRIORITY_WORK = 10000
PRIORITY_CLEANUP = 20000

CREATE_OBJECT = 'upload_object'  # includes obj name
READ_OBJECT = 'get_object'       # does NOT include obj name to get
UPDATE_OBJECT = 'update_object'  # does NOT include obj name to update
DELETE_OBJECT = 'delete_object'  # may or may not include obj name to delete
