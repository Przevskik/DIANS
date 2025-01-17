HOW TO RUN ? </br>
First navigate to Домашна_4 and run the issuer_service.py , then
open a new terminal and run python main.py . 

Dessign Pattern used : 
Strategy

This project consists of a set of microservices designed for data scraping, processing, and storage. The application is organized as follows: </br>

  main.py: Orchestrates the entire application, managing the flow between microservices.</br>
  annual_data_service.py: Handles the retrieval and processing of annual data for various issuers. </br>
  data_management_service.py: Manages the saving, retrieval, and storage of processed data. </br>
  issuer_service.py: Provides an API for fetching the list of issuers. </br>
