""" This application allows users to bulk label their GCP assets. """

#!/usr/bin/env python

# Copyright 2024 Google LLC.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
import yaml
import search
import model

def load_config(config_file='config.yaml'):
    """
    Load config from yaml file
    """
    with open(config_file, 'r', encoding='UTF-8') as f:
        conf = yaml.safe_load(f)
    return conf

config = load_config()


def main():
    """ Main entry point """
    project_id = config["project_id"]
    new_labels = config["labels"]
    search_asset_types = config["search_asset_types"]
    overwrite_existing = config["overwrite_existing"]

    assets = search.search_assets(project_id, search_asset_types)
    print("Asset discovery completed.")

    for asset in assets:
        print(f"Processing : {asset.name}  --> {asset.asset_type}")

        if asset.asset_type == "bigquery.googleapis.com/Dataset":
            asset = model.BigQueryDataset(project_id =project_id, asset=asset,
                                         labels=new_labels, replace_existing=overwrite_existing)
        elif asset.asset_type == "storage.googleapis.com/Bucket":
            asset = model.StorageBucket(project_id =project_id, asset=asset,
                                       labels=new_labels, replace_existing=overwrite_existing)
        elif asset.asset_type == "pubsub.googleapis.com/Topic":
            asset = model.PubsubTopic(project_id =project_id, asset=asset,
                                     labels=new_labels, replace_existing=overwrite_existing)
        elif asset.asset_type == "pubsub.googleapis.com/Subscription":
            asset = model.PubsubSubscription(project_id =project_id, asset=asset,
                                            labels=new_labels, replace_existing=overwrite_existing)
        elif asset.asset_type == "bigtableadmin.googleapis.com/Instance":
            asset = model.BigTableInstance(project_id =project_id, asset=asset,
                                           labels=new_labels, replace_existing=overwrite_existing)
        elif asset.asset_type == "compute.googleapis.com/Instance":
            asset = model.ComputeInstance(project_id =project_id, asset=asset,
                                          labels=new_labels, replace_existing=overwrite_existing)

        asset.update_asset_labels()
    print("Asset label updates completed.")

if __name__ == "__main__":
    main()
