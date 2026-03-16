from .direct_api_client import request


def get_ad(ad_id):

    result = request(
        "ads.get",
        {
            "SelectionCriteria": {
                "Ids": [ad_id]
            },
            "FieldNames": ["Id","AdGroupId","Status"],
            "TextAdFieldNames": ["Title","Title2","Text"]
        }
    )

    return result["Ads"][0]


def create_text_ad(ad_group_id, title, title2, text):

    result = request(
        "ads.add",
        {
            "Ads":[
                {
                    "AdGroupId": ad_group_id,
                    "TextAd":{
                        "Title": title,
                        "Title2": title2,
                        "Text": text
                    }
                }
            ]
        }
    )

    return result
