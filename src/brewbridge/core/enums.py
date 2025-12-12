from enum import Enum

class DefaultYAMLConfig(Enum):
    ACL = """
access_groups:
    # - "AADS_A_Brewdat-<zone/lz>-<np|p>-<zone>-<business_domain>-<subdomain/product/product_bu>-r" # Reader role access group
    # - "AADS_A_Brewdat-<zone/lz>-<np|p>-<zone>-<business_domain>-<subdomain/product/product_bu>-rw" # Collab role access group
    # - "AADS_A_Brewdat-<zone/lz>-<np|p>-<zone>-<business_domain>-<subdomain/product/product_bu>-s" # Sensitive role access group
"""
    METADATA = """
logical_name: #Required
description: "" #Required
update_frequency: #Nice to have
dataexpert: #Required
technical_data_steward: #Nice to have
business_data_steward: #Nice to have
table_sdi_classification: #Nice to have
is_pii: #Nice to have
brewdoc_link: #Optional
pii_reason: #Nice to have
tags: #Optional
  - ''
schema: #Required
  - name: #Required
    logical_name: #Optional
    description: #Required
    type: #Required
    primary_key: #Required
    sdi_classification: #Nice to have
    is_pii: #Nice to have
    cde: #Nice to have
"""
    OBSERVABILITY = """
type: 'ms_teams'
receivers:
    - "WebhookAlertsMAZMLP"
mentions:
    - user:
        user_email: ""
        display_name: ""
    - user:
        user_email: ""
        display_name: ""
"""
    SYNC = """
mode: 'append_only' # Default value for BRZ Layer is 'append_only'
# Available options for other layers are 'overwrite', 'append_merge', 'append_drop', 'append_only', 'upsert'.
"""
    TRANSFORMATION = """
# Default Transformations applied to BRZ and SLV layers: snakecase_column_names, add year,month,day partition columns (unless partition_by is specified on sync.yaml)
# Available options for both BRZ and SLV layers: column mapping (via target_name parameter in metadata.yaml), add source file name column, create derived partition columns.
# Available options for SLV layer: hash transformations, data cleansing (via regex), 
# add checkpoint column, drop duplicates, resolve duplicate columns, sort by columns.
"""
