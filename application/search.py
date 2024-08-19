
"""
This module contains methods to search GCP assets using Asset API.
"""
#pylint: disable=E0611
from google.cloud import asset_v1


def get_assets(project_id, asset_types):
    """
    This method returns asset class but we can't provide any query filters for the assets.
    Hence, I like using search_assets method for complex projects with lots of assets.
    """
    client = asset_v1.AssetServiceClient()

    # Call ListAssets v1 to list assets.
    assets = client.list_assets(
        request={
            "parent": f"projects/{project_id}",
            "read_time": None,
            "asset_types": asset_types,
            "content_type": asset_v1.ContentType.RESOURCE,
            "page_size": 0,
        }
    )
    return assets


def search_assets(project_id, asset_types):
    """
    This method is more suitable for projects with lots of objects.
    Biggest advantage of this method is, it allows us to search assets using a query field.
    """
    client = asset_v1.AssetServiceClient()
    # Initialize request argument(s)
    request = asset_v1.SearchAllResourcesRequest(
            scope=f"projects/{project_id}",
            read_mask="name,assetType,project",
            asset_types=asset_types,
            # https://cloud.google.com/asset-inventory/docs/reference/rest/v1/TopLevel/searchAllResources#query-parameters
            query="*",
        )

    # Make the request
    assets = client.search_all_resources(request=request)
    print(assets)
    return assets
