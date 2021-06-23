mappings = {
  "settings": {
    "number_of_shards": 1
  },
  "mappings": {
    "properties": {
      "plate": { "type": "text" },
      "state": { "type": "text" },
      "license_type": { "type": "text" },
      
      "judgment_entry_date": { "type": "date" },
      "violation": { "type": "text" },
      "precint": { "type": "date" },
      "county": { "type": "text" },
      "violation_status": { "type": "text" },
      "issuing_agency": { "type": "text" },
      "summons_image":{
          "properties":{
              "url":{"type":"string"}},
        "description":{"type":"text"},
      
      "violation_time": { "type": "date" },
      "issue_date": { "type": "date", "format":"yyyy-MM-dd" },
      
      "amount_due": { "type": "float" },
      "payment_amount": { "type": "float" },
      "reduction_amount": { "type": "float" },
      "interest_amount": { "type": "float" },
      "penalty_amount": { "type": "float" },
      "fine_amount": { "type": "float" },
      "summons_number": { "type": "float" },
      }
    }
  }
}