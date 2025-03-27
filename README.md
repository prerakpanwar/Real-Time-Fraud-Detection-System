![Untitled-2025-03-15-2019](https://github.com/user-attachments/assets/2c7ae7d9-d710-46ba-ae02-52367f1601ce)

# Real-Time-Fraud-Detection-System 

"The project is in Progress"

Tools used: Docker, Kafka, Mysql, Python, VsCode, Jupyter labs

Phase 1 (Testing basic connections: completed)

Consistes of :
1-Testing Real time transactions data is getting produced using kafka producer.
2-Testing that the transactions data sent is getting consumed successfully by kafka consumer.
3-Before getting consumed, made a Risk Profile for the users in a table in MySQL.
4-Storing the consumed raw transactions in a table in MySQL successfully.

Information under can change over time as per the new modifications to the project:

Flow of the Project:
I will be making 3 systems:
1- Producer system (PS)
2- Risk system (RS)
3- ML model (model)

PS will Producing transactions --> RS will Receive transactions, Call the ML model, Model will predict fraud/ not fraud, then store the predicted result in MySQL and give alert --> then for correcting the FN and FP results I will feed the feedback to ML model again for model improvement.
