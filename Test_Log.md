### Extraction Test ### 
Removing existing CSV file exists <br />Confirming that CSV file doesn't exists... <br />Test Successful <br />Extracting data and saving... <br />Testing if CSV file exists... <br />Extraction Test Successful


### Transform and Load Test ### 
Creating non-lookup table: air_quality <br />Creating lookup table: indicator <br />Creating lookup table: geo_data <br />Tables created. <br />Skipping the first row... <br />Inserting table data... <br />Inserting table data completed <br />Transform and Load Test Successful


### One Record Reading Test ### 

```sql
select * from air_quality where air_quality_id = 740885
```

One Record Reading Test Successful


### All Records Reading Test ### 
All Records Reading Test Successful


### Record Saving Test ### 
Record Saving Test Successful


### Record Update Test ### 
Record Update Test Successful


### Record Deletion Test ### 
Record Deletion Test Successful


### Reading All Column Test ### 
Reading All Column Test Successful


