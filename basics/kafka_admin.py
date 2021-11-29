from logging import Logger
from typing import List

from confluent_kafka.admin import AdminClient, GroupMetadata, GroupMember

from logger.logger import get_logger

group_id = "test-group-id"
logger: Logger = get_logger()
admin_client: AdminClient = AdminClient({"bootstrap.servers": "localhost:9092"})
group_metadata_list: List[GroupMetadata] = admin_client.list_groups(group_id)
if len(group_metadata_list) == 0:
    logger.debug(f"No consumers for group-id: {group_id}")
else:
    group_metadata: GroupMetadata = group_metadata_list[0]
    logger.debug(f"GroupMetadata: {group_metadata.__dict__}")
    group_members_list: List[GroupMember] = group_metadata.members
    if group_members_list is None or len(group_members_list) == 0:
        logger.debug(f"No consumers for group-id: {group_id}")
    else:
        logger.debug(f"There are {len(group_members_list)} current consumers")
        for group_member in group_members_list:
            logger.debug(f"Group Member: {group_member.__dict__}")
