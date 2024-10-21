# This file is to be replaced with Data Prov to retreive the query result with provenance annotations 

# Provenance annotations: it is assumed that the polynomials are expanded:
#   >  "t1 * t1" instead of "t1^2"
#   >  "t1 x t2 + t1 x t3" instead of "t1 x (t2 + t3)"
#   > "*" means scalar multiplication

# Each variable columnsN is always a tuple with 2 values:
#   > a list with the names of all 'regular' columns, i.e., except the aggregation columns. Use the table name  
#     to deal with ambiguous column names. 
#   > a list with the name of the aggregation columns.
#     In this case, the column name is not important. Only the number of aggregations matters.
# The regular columns come first and the aggregation columns must be in the end.

### Example 1
query1 = """
    SELECT model, cast(SUM(num_events) as INTEGER) AS num_events
    FROM (
        SELECT 	model, COUNT(*) as num_events
        FROM	TE_azores A INNER JOIN Equipments E on A.sn = E.sn
        GROUP BY model 
            UNION
        SELECT model, num_events
        FROM TE_madeira
    ) AS TE
    GROUP BY model
"""
columns1 = (['model'], ['num_events']) 
provAnnotation1 = ["TE_madeira:t10 * 7 + TE_azores:t4 * Equipments:t5 * 1 + TE_azores:t1 * Equipments:t5 * 1 + TE_azores:t2 * Equipments:t6 * 1" , "TE_azores:t3 * Equipments:t7 * 1 + TE_madeira:t8 * 10 + TE_madeira:t9 * 5"]


### Example 2
query2 = """
	    SELECT 	A.sn, model, COUNT(*) as num_events
        FROM	TE_azores A INNER JOIN Equipments E on A.sn = E.sn
        GROUP BY A.sn, model 
"""
columns2 = (['TE_azores.sn', 'model'], ['num_events']) 
provAnnotation2 = ["TE_azores:t2 * Equipments:t6 * 1", "TE_azores:t4 * Equipments:t5 * 1 + TE_azores:t1 * Equipments:t5 * 1" , "TE_azores:t3 * Equipments:t7 * 2"] 


# my functions
def getQuery(num):
    if num == 1:
        return query1, columns1, provAnnotation1
    elif num == 2:
        return query2, columns2, provAnnotation2
    else:
        return None, None