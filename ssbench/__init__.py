#
#Copyright (c) 2012-2021, NVIDIA CORPORATION.
#SPDX-License-Identifier: Apache-2.0

_version = (0, 3, 9)
version = '.'.join(map(str, _version))

# Constant names (which happen to need to match methods called
# handle_<OPERATION> in worker.py)
CREATE_OBJECT = 'upload_object'
READ_OBJECT = 'get_object'
UPDATE_OBJECT = 'update_object'
DELETE_OBJECT = 'delete_object'
