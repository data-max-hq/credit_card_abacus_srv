# credit_card_abacus_srv
To Do's regarding Centaur Service:
 - Create test atmp_t17 and atmp_18 tables on RDS DB so we dont interfere with spark functions testing.
 - Change the table names in the following functions to match the test table names: truncate_atmp_t17_dpd_credit_cards; truncate_atmp_t18_cc_payment_schedule; bulk_insert_atmp_t17; bulk_insert_atmp_t18 . You will find all of these within the cc_abacus_da.py  script.
 - Test the service by running main.py. Ideally data should be inserted on the newly created test tables. There is most probably a need to hardcore some values in the rds db so the working day matches the actual day when the test is conducted.
 - If there is problem with the external Centaur Service coming from this url http://dev2012test:808/CentaurServiceDev/CentaurService.asmx  the point of contact from Raiffeisen should be Aurela.
 - Finally go ahead with the ci/cd for eks deployment. Remarks: Should be a cron job that runs every 20 min.
 - I would personally store the connection string in AWS Secret Manager and configure EKS to read it from there but do as you see more fit :slightly_smiling_face: .
The Raiffeisen repo with the C# code is named: rbal-risk-abacus-credit-card . The corresponding files for main.py and credit_card_abacus_service.py are found in the CreditCard_Abacus_Srv folder in this repo and the rest are found in CreditCard_Abcus_DA.


CentaurServiceUrl="http://dev2012test:808/CentaurServiceDev/CentaurService.asmx"
AbacusConStr="jdbc:postgresql://ctabacus-poenards.rbal-test-al1.rbigroup.internal:5432/postgres?user=postgres-user&password=sdfasdfd"
SchemaUsed="ABACUS_A5"
CentaurConStr="jdbc:sqlserver://;serverName=ctcentaurrds.cs5v1okucqry.eu-central-1.rds.amazonaws.com;databaseName=master;integrateddSecurity=true;"
