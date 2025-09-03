X={
            
            "relationships": {
                "credit": {
                    "data": {
                        "type": "credits",
                        "id": "2593"
                    }
                },
                "user": {
                    "data": {
                        "type": "users",
                        "id": "149246"
                    }
                },
                "broker": {
                    "data": {
                        "type": "users",
                        "id": "35297"
                    }
                },
                "parent_credit_transaction": {
                    "data": {
                        "type": "credit_transactions",
                        "id": "2152"
                    }
                }
            }
        }


print(X.get("relationships", {}).get("parent_credit_transaction", {}).get("data", {}).get("type"))
