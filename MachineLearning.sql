WITH MachineLearning AS (
SELECT
    i.AssetId,
    i.DataPointId,
    i.Value,
    System.Timestamp AS [Timestamp],
     a.DataPoints
FROM
    IotEvents i TIMESTAMP BY MeasurementTime
JOIN AssetInfo a
    On i.AssetId = a.AssetId 
WHERE 
    a.SourceDataType = 'MachineLearning'
	and (GetType(i.Value) = 'bigint' OR GetType(i.Value) = 'float')
)
,MachineLearningAverages AS (
-- Calculate average over 1 hour here
SELECT CONCAT(m.AssetId,';',m.DataPointId,';',CAST(System.Timestamp AS nvarchar(max)),';','PT1H') AS id,
    m.AssetId,
    m.DataPointId,
     m.DataPoints,
    DATEADD(hour, -1, System.Timestamp) AS [Start],    
    System.Timestamp AS [End],
    System.Timestamp AS [Timestamp],
    'PT1H' AS Period,
    AVG(m.Value) AS Value,
    MIN(m.Value) AS Min, -- Minimum value
    MAX(m.Value) AS Max  -- Maximum value
FROM
    MachineLearning m
GROUP BY m.AssetId, m.DataPointId, m.DataPoints, TumblingWindow(hour, 1))
,Averages AS (
Select a.id,
    a.AssetId,
    a.DataPointId,
    sensorCode.ArrayValue.HasConditionIndicator as HasConditionIndicator,
    a.Start,
    a.[End],
    a.[Timestamp],
    a.Period,
    a.[Value],
    a.Min,
    a.Max
From MachineLearningAverages a
CROSS APPLY 
     GetArrayElements(a.DataPoints) AS sensorCode
WHERE
     sensorCode.ArrayValue.DataPointId = a.DataPointId
)
,Raw As (
SELECT CONCAT(m.AssetId,';',m.DataPointId,';',CAST(m.[Timestamp] AS nvarchar(max)),';','RAW') AS id,
    m.AssetId,
    m.DataPointId,
     sensorCode.ArrayValue.HasConditionIndicator as HasConditionIndicator,
    NULL AS Start, NULL AS [End], m.[Timestamp],
    'RAW' AS Period,
    m.Value,
    NULL AS Min,
    NULL AS Max
FROM
    MachineLearning m
CROSS APPLY 
     GetArrayElements(m.DataPoints) AS sensorCode
WHERE
     sensorCode.ArrayValue.DataPointId = m.DataPointId
)

