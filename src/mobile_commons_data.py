### an extremely hacky temporary workaround -- I wanted to avoid running into relative path issues when running
### using Airflow or Civis

class columns:
    def __init__(self):
        self.columns = {
        	"profiles": {
        		"id" : "float64",
        		"first_name" : "str",
        		"last_name" : "str",
        		"phone_number" : "str",
        		"email" : "str",
        		"status" : "str",
        		"created_at" : "datetime64[ns, UTC]",
        		"updated_at" : "datetime64[ns, UTC]",
        		"opted_out_at" : "datetime64[ns, UTC]",
        		"opted_out_source" : "str",
                "source": "str",
                "address" : "str",
                "last_saved_districts" : "str",
                "last_saved_location" : "str"

        	},

        	"groups": {
        		"id" : "float64",
        		"type" : "str",
        		"status" : "str",
        		"name" : "str",
        		"size" : "float64"
        	},

        	"group_members": {
        		"id" : "float64",
        		"email" : "str",
        		"first_name" : "str",
        		"last_name" : "str",
        		"status" : "str",
        		"created_at" : "datetime64[ns, UTC]",
                "updated_at": "datetime64[ns, UTC]",
        		"opted_out_at" : "datetime64[ns, UTC]",
        		"opted_out_source": "str",
        		"group_id": "float64",
                "source":"str"
        	},

        	"campaigns": {
        		"id" : "float64",
        		"name" : "str",
        		"active" : "bool",
        		"description" : "str" ,
        		"tags" : "str",
                "opt_in_paths":"str"
        	},

        	"campaign_subscribers": {
        		"id" : "float64",
        		"profile_id" : "float64",
        		"phone_number" : "str",
        		"activated_at" : "datetime64[ns, UTC]",
        		"opted_out_at" : "datetime64[ns, UTC]",
        		"campaign_id": "float64"
        	},

        	"messages": {
        		"approved" : "bool",
        		"id" : "float64",
        		"type" : "str",
        		"body" : "str",
        		"campaign" : "str",
        		"campaign_id": "float64",
        		"carrier_name" : "str",
        		"keyword" : "str",
        		"message_template_id": "float64",
        		"mms" : "bool",
        		"multipart" : "bool",
        		"next_id" : "str",
        		"phone_number" : "str",
        		"previous_id" : "str",
        		"profile" : "float64",
        		"received_at" : "datetime64[ns, UTC]"
        	},

        	"sent_messages": {
        		"id" : "float64",
        		"status" : "str",
        		"type" : "str",
        		"body" : "str",
        		"campaign" : "str",
        		"campaign_id": "float64",
        		"message_template_id" : "float64",
        		"mms"  : "bool",
        		"multipart" : "bool",
        		"next_id" : "str",
        		"phone_number" : "str",
        		"previous_id" : "str",
        		"profile" : "str",
        		"sent_at" : "datetime64[ns, UTC]"
        	},

        	"broadcasts": {
        		"id" : "float64",
        		"status" : "str",
        		"automated" : "bool",
        		"body" : "str",
        		"campaign" : "str",
        		"delivery_time" : "datetime64[ns, UTC]",
        		"estimated_recipients_count" : "float64",
        		"excluded_groups" : "str",
        		"include_subscribers" : "bool",
        		"included_groups" : "str",
        		"localtime" : "bool",
        		"name" : "str",
        		"opt_outs_count" : "float64",
        		"replies_count" : "float64",
        		"tags" : "str",
        		"throttled" : "bool"
        	},

        	"tags": {
        		"id" : "float64",
        		"name" : "str",
        		"taggable_object" : "str"

        	},

        	"tinyurls": {
        		"id" : "float64",
        		"created_at" : "datetime64[ns, UTC]",
        		"description" : "str",
        		"host" : "str",
        		"key" : "str",
        		"mode" : "str",
        		"name" : "str",
        		"url" : "str"
        	},

        	"clicks": {
        		"id" : "float64",
        		"clicked_url" : "str",
        		"created_at"  : "datetime64[ns, UTC]",
        		"http_referer" : "str",
        		"remote_addr" : "str",
        		"url" : "str",
        		"user_agent" : "str"
        	}

        }
