from .direct_api_client import request

def add_negative_keywords(campaign_id, words):

    request(
        "campaigns.update",
        {
            "Campaigns":[
                {
                    "Id": campaign_id,
                    "NegativeKeywords":{
                        "Items": words.split()
                    }
                }
            ]
        }
    )
