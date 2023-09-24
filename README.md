Data engineering project that aims to anaylise Uber traffic data. It consists of three parts : <br/>
1- Ingest data from CSV file to GCP infrastructure <br/>
2- Configure GCP infrastructure to create new VM and create pipeline using compute engine <br/>
3- Import Raw data to Google storage which consists a staging ground enabling data import into the Cloud <br/>
4- Create pipeline to load-transform-export data to Bigquery Datawarehouse <br/>
5- Querying data using Bigquery standard SQL <br/>
6- Create a dashboard to visualise results of analysis inside Looker studio tool <br/>

Technologies: <br/>
1- Google cloud plateform <br/>
2- Mage.io <br/>
3- Looker studio <br/>
5- Python <br/>

Here is the global architecture of this project :<br/>
![architecture](https://github.com/Soukaina-DOUKKANI/GCP-Mage-BQ-DE-project/assets/73255489/76fb246e-a688-434f-a840-759476fbc087)<br/>

And for that import, we used a schema in stars composed of fact table and several dimensions related to passengers, trips, pickup and dropoff duration and payment type <br/>
![data_model](https://github.com/Soukaina-DOUKKANI/GCP-Mage-BQ-DE-project/assets/73255489/5d471305-51e9-471a-85a8-9d24f9a5cb1b)<br/>

List of references:<br/>
[1]:https://www.youtube.com/watch?v=WpQECq5Hx9g<br/>
[2]:https://k21academy.com/google-cloud/create-google-cloud-free-tier-account/<br/>
