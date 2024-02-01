{"version":"NotebookV1","origId":1965691013914853,"name":"Saving_Data","language":"scala","commands":[{"version":"CommandV1","origId":1965691013914854,"guid":"c578548d-f8bb-43c8-8836-ad4c855409a7","subtype":"command","commandType":"auto","position":1.0,"command":"%run ./Create_DataFrames","commandVersion":9,"state":"error","results":null,"errorSummary":null,"error":null,"workflows":[],"startTime":0,"submitTime":0,"finishTime":0,"collapsed":true,"bindings":{},"inputWidgets":{},"displayType":"table","width":"auto","height":"auto","xColumns":null,"yColumns":null,"pivotColumns":null,"pivotAggregation":null,"useConsistentColors":false,"customPlotOptions":{},"commentThread":[],"commentsVisible":false,"parentHierarchy":[],"diffInserts":[],"diffDeletes":[],"globalVars":{},"latestUser":"wadson@insightahead.com","latestUserId":"4218386567210383","commandTitle":"","showCommandTitle":false,"hideCommandCode":false,"hideCommandResult":false,"isLockedInExamMode":false,"iPythonMetadata":null,"streamStates":{},"datasetPreviewNameToCmdIdMap":{},"nuid":"2b02d361-ee5a-4dac-945a-ec80b5f6954f"},{"version":"CommandV1","origId":1965691013914855,"guid":"92571023-bd88-49cd-a187-ecd251ba1e06","subtype":"command","commandType":"auto","position":2.0,"command":"import org.apache.spark.sql.functions._","commandVersion":11,"state":"error","results":null,"errorSummary":null,"error":null,"workflows":[],"startTime":0,"submitTime":0,"finishTime":0,"collapsed":true,"bindings":{},"inputWidgets":{},"displayType":"table","width":"auto","height":"auto","xColumns":null,"yColumns":null,"pivotColumns":null,"pivotAggregation":null,"useConsistentColors":false,"customPlotOptions":{},"commentThread":[],"commentsVisible":false,"parentHierarchy":[],"diffInserts":[],"diffDeletes":[],"globalVars":{},"latestUser":"wadson@insightahead.com","latestUserId":"4218386567210383","commandTitle":"","showCommandTitle":false,"hideCommandCode":false,"hideCommandResult":false,"isLockedInExamMode":false,"iPythonMetadata":null,"streamStates":{},"datasetPreviewNameToCmdIdMap":{},"nuid":"06705940-50aa-48e3-be92-eefe01858c50"},{"version":"CommandV1","origId":1965691013914856,"guid":"feddb6a9-6ab5-4c4e-a576-94b5c376565a","subtype":"command","commandType":"auto","position":2.5,"command":"val customerWithAddress = customerDf\n.na.drop(\"any\")\n.join(addressDf, customerDf(\"address_id\") === addressDf(\"address_id\"))\n.select('customer_id, \n        'demographics,\n        concat_ws(\" \",'firstname, 'lastname).as(\"Name\"),\n        addressDf(\"*\")\n       )","commandVersion":135,"state":"error","results":null,"errorSummary":null,"error":null,"workflows":[],"startTime":0,"submitTime":0,"finishTime":0,"collapsed":false,"bindings":{},"inputWidgets":{},"displayType":"table","width":"auto","height":"auto","xColumns":null,"yColumns":null,"pivotColumns":null,"pivotAggregation":null,"useConsistentColors":false,"customPlotOptions":{},"commentThread":[],"commentsVisible":false,"parentHierarchy":[],"diffInserts":[],"diffDeletes":[],"globalVars":{},"latestUser":"wadson@insightahead.com","latestUserId":"4218386567210383","commandTitle":"","showCommandTitle":false,"hideCommandCode":false,"hideCommandResult":false,"isLockedInExamMode":false,"iPythonMetadata":null,"streamStates":{},"datasetPreviewNameToCmdIdMap":{},"nuid":"6be65e08-bfdc-43b7-8b3b-80ad55780422"},{"version":"CommandV1","origId":1965691013914857,"guid":"7e533cbf-579d-4d5d-aa42-8de3c7c22754","subtype":"command","commandType":"auto","position":2.65625,"command":"val salesWithItem = webSalesDf\n.na.drop(\"any\")\n.join(itemDf,$\"i_item_sk\" === $\"ws_item_sk\")\n.select(\n            $\"ws_bill_customer_sk\".as(\"customer_id\"),\n            $\"ws_ship_addr_sk\".as(\"ship_address_id\"),\n            $\"i_product_name\".as(\"item_name\"),\n            trim($\"i_category\").as(\"item_category\"),\n            $\"ws_quantity\".as(\"quantity\"),\n            $\"ws_net_paid\".as(\"net_paid\")\n           )\n","commandVersion":179,"state":"error","results":null,"errorSummary":null,"error":null,"workflows":[],"startTime":0,"submitTime":0,"finishTime":0,"collapsed":false,"bindings":{},"inputWidgets":{},"displayType":"table","width":"auto","height":"auto","xColumns":null,"yColumns":null,"pivotColumns":null,"pivotAggregation":null,"useConsistentColors":false,"customPlotOptions":{},"commentThread":[],"commentsVisible":false,"parentHierarchy":[],"diffInserts":[],"diffDeletes":[],"globalVars":{},"latestUser":"wadson@insightahead.com","latestUserId":"4218386567210383","commandTitle":"","showCommandTitle":false,"hideCommandCode":false,"hideCommandResult":false,"isLockedInExamMode":false,"iPythonMetadata":null,"streamStates":{},"datasetPreviewNameToCmdIdMap":{},"nuid":"fbc4e7b0-fb7f-4e43-849f-fd3bd5917ae2"},{"version":"CommandV1","origId":1965691013914858,"guid":"8d1cd534-75d9-4cf6-8283-6b46e06b1988","subtype":"command","commandType":"auto","position":2.6953125,"command":"val customerPurchases = salesWithItem\n.join(customerWithAddress, salesWithItem(\"customer_id\") === customerWithAddress(\"customer_id\"))\n.select(customerWithAddress(\"*\"),\n        salesWithItem(\"*\"))\n.drop(salesWithItem(\"customer_id\"))","commandVersion":58,"state":"error","results":null,"errorSummary":null,"error":null,"workflows":[],"startTime":0,"submitTime":0,"finishTime":0,"collapsed":false,"bindings":{},"inputWidgets":{},"displayType":"table","width":"auto","height":"auto","xColumns":null,"yColumns":null,"pivotColumns":null,"pivotAggregation":null,"useConsistentColors":false,"customPlotOptions":{},"commentThread":[],"commentsVisible":false,"parentHierarchy":[],"diffInserts":[],"diffDeletes":[],"globalVars":{},"latestUser":"wadson@insightahead.com","latestUserId":"4218386567210383","commandTitle":"","showCommandTitle":false,"hideCommandCode":false,"hideCommandResult":false,"isLockedInExamMode":false,"iPythonMetadata":null,"streamStates":{},"datasetPreviewNameToCmdIdMap":{},"nuid":"0e9dee5c-4b54-4d3e-914c-7d4fcd10691a"},{"version":"CommandV1","origId":1965691013914859,"guid":"863e6eb9-fafe-4cf0-9546-154d6f32fa65","subtype":"command","commandType":"auto","position":3.6953125,"command":"display(customerPurchases)","commandVersion":8,"state":"error","results":null,"errorSummary":null,"error":null,"workflows":[],"startTime":0,"submitTime":0,"finishTime":0,"collapsed":false,"bindings":{},"inputWidgets":{},"displayType":"table","width":"2752.85546875","height":"auto","xColumns":null,"yColumns":null,"pivotColumns":null,"pivotAggregation":null,"useConsistentColors":false,"customPlotOptions":{},"commentThread":[],"commentsVisible":false,"parentHierarchy":[],"diffInserts":[],"diffDeletes":[],"globalVars":{},"latestUser":"wadson@insightahead.com","latestUserId":"4218386567210383","commandTitle":"","showCommandTitle":false,"hideCommandCode":false,"hideCommandResult":false,"isLockedInExamMode":false,"iPythonMetadata":null,"streamStates":{},"datasetPreviewNameToCmdIdMap":{},"nuid":"8c4d6e25-9d34-4925-ae85-dd9d8958e260"},{"version":"CommandV1","origId":1965691013914860,"guid":"0ae6ec2c-c950-4ff3-b641-07d3be5a3bb7","subtype":"command","commandType":"auto","position":4.1953125,"command":"","commandVersion":0,"state":"error","results":null,"errorSummary":null,"error":null,"workflows":[],"startTime":0,"submitTime":0,"finishTime":0,"collapsed":false,"bindings":{},"inputWidgets":{},"displayType":"table","width":"auto","height":"auto","xColumns":null,"yColumns":null,"pivotColumns":null,"pivotAggregation":null,"useConsistentColors":false,"customPlotOptions":{},"commentThread":[],"commentsVisible":false,"parentHierarchy":[],"diffInserts":[],"diffDeletes":[],"globalVars":{},"latestUser":"","latestUserId":"4218386567210383","commandTitle":"","showCommandTitle":false,"hideCommandCode":false,"hideCommandResult":false,"isLockedInExamMode":false,"iPythonMetadata":null,"streamStates":{},"datasetPreviewNameToCmdIdMap":{},"nuid":"3355a31a-0401-47b4-9d45-856685f4fd21"},{"version":"CommandV1","origId":1965691013914861,"guid":"781195ec-adc9-43a3-ac35-124279c54e77","subtype":"command","commandType":"auto","position":4.6953125,"command":"customerPurchases\n.select($\"*\",\n       trim($\"item_category\").as(\"it\"))\n.drop(\"item_category\")\n.withColumnRenamed(\"it\",\"item_category\")\n.repartition(8)\n.write\n.partitionBy(\"item_category\")\n.option(\"compression\",\"lz4\")\n.mode(SaveMode.Overwrite)\n.option(\"path\",\"tmp/output/customerPurchases\")\n.save","commandVersion":169,"state":"error","results":null,"errorSummary":null,"error":null,"workflows":[],"startTime":0,"submitTime":0,"finishTime":0,"collapsed":false,"bindings":{},"inputWidgets":{},"displayType":"table","width":"auto","height":"auto","xColumns":null,"yColumns":null,"pivotColumns":null,"pivotAggregation":null,"useConsistentColors":false,"customPlotOptions":{},"commentThread":[],"commentsVisible":false,"parentHierarchy":[],"diffInserts":[],"diffDeletes":[],"globalVars":{},"latestUser":"wadson@insightahead.com","latestUserId":"4218386567210383","commandTitle":"","showCommandTitle":false,"hideCommandCode":false,"hideCommandResult":false,"isLockedInExamMode":false,"iPythonMetadata":null,"streamStates":{},"datasetPreviewNameToCmdIdMap":{},"nuid":"24df647d-f469-4714-98df-c595d9e9d1ac"},{"version":"CommandV1","origId":1965691013914862,"guid":"7101e128-6a98-4cbf-ac64-89242982c9d9","subtype":"command","commandType":"auto","position":5.0703125,"command":"customerPurchases.select(length($\"item_category\")).show","commandVersion":16,"state":"error","results":null,"errorSummary":null,"error":null,"workflows":[],"startTime":0,"submitTime":0,"finishTime":0,"collapsed":false,"bindings":{},"inputWidgets":{},"displayType":"table","width":"auto","height":"auto","xColumns":null,"yColumns":null,"pivotColumns":null,"pivotAggregation":null,"useConsistentColors":false,"customPlotOptions":{},"commentThread":[],"commentsVisible":false,"parentHierarchy":[],"diffInserts":[],"diffDeletes":[],"globalVars":{},"latestUser":"wadson@insightahead.com","latestUserId":"4218386567210383","commandTitle":"","showCommandTitle":false,"hideCommandCode":false,"hideCommandResult":false,"isLockedInExamMode":false,"iPythonMetadata":null,"streamStates":{},"datasetPreviewNameToCmdIdMap":{},"nuid":"4f09c4d4-03ce-4442-9ac7-123fb36b2e1f"},{"version":"CommandV1","origId":1965691013914863,"guid":"43bc91d7-aee1-4494-8287-d589459a84c2","subtype":"command","commandType":"auto","position":5.4453125,"command":"%fs ls /tmp/output/customerPurchases/item_category=Books/","commandVersion":3,"state":"error","results":null,"errorSummary":null,"error":null,"workflows":[],"startTime":0,"submitTime":0,"finishTime":0,"collapsed":false,"bindings":{},"inputWidgets":{},"displayType":"table","width":"1561.74365234375","height":"auto","xColumns":null,"yColumns":null,"pivotColumns":null,"pivotAggregation":null,"useConsistentColors":false,"customPlotOptions":{},"commentThread":[],"commentsVisible":false,"parentHierarchy":[],"diffInserts":[],"diffDeletes":[],"globalVars":{},"latestUser":"wadson@insightahead.com","latestUserId":"4218386567210383","commandTitle":"","showCommandTitle":false,"hideCommandCode":false,"hideCommandResult":false,"isLockedInExamMode":false,"iPythonMetadata":null,"streamStates":{},"datasetPreviewNameToCmdIdMap":{},"nuid":"398006a3-10b4-4fd1-85e5-f709f2966dd3"},{"version":"CommandV1","origId":1965691013914864,"guid":"046df15c-9d2c-48d0-908e-5b9fab7732d4","subtype":"command","commandType":"auto","position":5.6953125,"command":"%fs ls tmp/output/customerPurchases","commandVersion":6,"state":"error","results":null,"errorSummary":null,"error":null,"workflows":[],"startTime":0,"submitTime":0,"finishTime":0,"collapsed":false,"bindings":{},"inputWidgets":{},"displayType":"table","width":"1555.2373046875","height":"auto","xColumns":null,"yColumns":null,"pivotColumns":null,"pivotAggregation":null,"useConsistentColors":false,"customPlotOptions":{},"commentThread":[],"commentsVisible":false,"parentHierarchy":[],"diffInserts":[],"diffDeletes":[],"globalVars":{},"latestUser":"wadson@insightahead.com","latestUserId":"4218386567210383","commandTitle":"","showCommandTitle":false,"hideCommandCode":false,"hideCommandResult":false,"isLockedInExamMode":false,"iPythonMetadata":null,"streamStates":{},"datasetPreviewNameToCmdIdMap":{},"nuid":"044e621b-2d36-4a2d-a3e3-9c5718f2e4ac"},{"version":"CommandV1","origId":1965691013914865,"guid":"ec707d80-4187-4e7f-994d-dae7f169695a","subtype":"command","commandType":"auto","position":6.6953125,"command":"spark.read.parquet(\"dbfs:/tmp/output/customerPurchases/item_category=Books\")","commandVersion":8,"state":"error","results":null,"errorSummary":null,"error":null,"workflows":[],"startTime":0,"submitTime":0,"finishTime":0,"collapsed":false,"bindings":{},"inputWidgets":{},"displayType":"table","width":"auto","height":"auto","xColumns":null,"yColumns":null,"pivotColumns":null,"pivotAggregation":null,"useConsistentColors":false,"customPlotOptions":{},"commentThread":[],"commentsVisible":false,"parentHierarchy":[],"diffInserts":[],"diffDeletes":[],"globalVars":{},"latestUser":"wadson@insightahead.com","latestUserId":"4218386567210383","commandTitle":"","showCommandTitle":false,"hideCommandCode":false,"hideCommandResult":false,"isLockedInExamMode":false,"iPythonMetadata":null,"streamStates":{},"datasetPreviewNameToCmdIdMap":{},"nuid":"bd6e6aed-d4c5-40ee-9c4c-e14956601ff8"}],"dashboards":[],"guid":"c0930e81-9062-4be4-8ae5-39bf9647fdf6","globalVars":{},"iPythonMetadata":null,"inputWidgets":{}}