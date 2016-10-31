Navigator Optimizer Anonymizer
========

## Synopsis

Anonymizer is a tool that is designed to be used in conjunction with the Navigator Optimizer product.

Given a SQL workload (.csv or semicolon-separated .sql), it can:

1. Mask all literals in the SQL queries
2. Encrypt table and column names

## Examples

### Anonymize .sql workload
```
java -jar target/navopt-workload-anonymizer-0.1-SNAPSHOT.jar -i /tmp/input.sql -sql
```
```
Input file is : /tmp/input.sql
Output file is : /tmp/anoninput.sql
Key file is : /tmp/anoninput.sql.passkey
Error rows/queries collected at : /tmp/anoninput.sql.err
```
### De-anonymize .sql workload
```
java -jar target/navopt-workload-anonymizer-0.1-SNAPSHOT.jar -i /tmp/anoninput.sql -sql -k /tmp/anoninput.sql.passkey -d
```
```
Input file is : /tmp/anoninput.sql
Output file is : /tmp/deanonanoninput.sql
Key file is : /tmp/anoninput.sql.passkey
Error rows/queries collected at : /tmp/deanonanoninput.sql.err
```
### Anonymize .csv workload
```
java -jar target/navopt-workload-anonymizer-0.1-SNAPSHOT.jar -i /tmp/input.csv -q 2
```
```
Input file is : /tmp/input.csv
Output file is : /tmp/anoninput.csv
Key file is : /tmp/anoninput.csv.passkey
Error rows/queries collected at : /tmp/anoninput.csv.err
```
### De-anonymize .csv workload
```
java -jar target/navopt-workload-anonymizer-0.1-SNAPSHOT.jar -i /tmp/anoninput.csv -q 2 -k /tmp/anoninput.csv.passkey -d
```
```
Input file is : /tmp/anoninput.csv
Output file is : /tmp/deanonanoninput.csv
Key file is : /tmp/anoninput.csv.passkey
Error rows/queries collected at : /tmp/deanonanoninput.csv.err
```

## Motivation

The anonymizer consists of two components, which serve different purposes. 

### Literal stripping 
>Security standards such as PCI-DSS and laws such as HIPAA mandate that PII (Personal Identifiable Information) and PHI (Protected Health Information) is treated with the highest level of confidentiality. PII/PHI is defined as information that can unambiguously identify an individual. In the case of HIPAA, for example, this could be names, addresses, or social security numbers. This kind of information is almost never in SQL queries. The second highest level of confidentiality protects what is called a limited data set. This is information such as birth date, geographical region, or hospital admission dates, that could be used to identify an individual within a population. This information can sometimes be contained within the literals of SQL queries. The Anonymizer tool (run client-side) irreversibly strips these literals from the SQL queries before sending them to the NavOpt cloud service. The NavOpt cloud service only sees these de-identified queries. 

#### Example use case
>A health-care organization wants to make sure all PHI data is scrubbed from the SQL queries before being sent to the NavOpt service. Anonymizer gives peace of mind that all data that is transmitted has been completely scrubbed of any sensitive data. 

### Schema obfuscation
>Sometimes, the schema of the database can also be sensitive information. Schema is sometimes treated as IP because it could reveal how a company organizes its data. Also, column names, if descriptive, can reveal what data a company is collecting, which could be sensitive. Anonymizer encrypts all column names and table names in the SQL queries client-side with a secret password only known to the user, before sending the queries to the NavOpt cloud service. The encrypted SQL queries retain their structure (SQL keywords are in plaintext) but all table names and column names are encrypted with state-of-the-art encryption standards. NavOpt is a zero-knowledge service provider, because any private or sensitive information that is received will always be in an encrypted form protected by a password, that is never transferred to NavOpt or anyone else. If the user forgets their password or loses their key, no recovery of the data is possible. There is no “password-reset” feature. Schema change recommendations are computed on obfuscated table and column names, and the user views these recommendations by supplying a password (which is never sent to the NavOpt cloud service). The decryption happens securely in the browser, client-side. 

#### Example use case:
>A company uses a secret proprietary algorithm that uses public Facebook profiles to make decisions about its customers. A column name titled “facebook-id” would reveal that the company is looking at facebook information. Anonymizer ensures that this information remains with the company by encrypting all column and table names client-side.  


## Usage with Navigator Optimizer product

1. The user supplies a passcode to the CLI that is used to encrypt the SQL log files.  The encryption happens client-side (no data transmission over network). The encrypted SQL queries retain their structure (SQL keywords are in plaintext) but all table names and column names are encrypted, and literals are entirely dropped. The CLI generates a .passkey file that will be necessary for recovery of encrypted schema information. 
2. The encrypted workload is analyzed by Navigator Optimizer (SaaS product). The analysis is possible because Navigator Optimizer only uses the structure of the queries to make recommendations, not the data itself. Literals are not sent to the cloud service at all (whether encrypted or unencrypted), and so they can never be recovered post-anonymization. 
3. Decryption of the data to see column names and table names happens on the client-side (in the browser). No decryption is possible without the passcode and .passkey file, and they are never sent to the NavOpt service (decryption happens in the Javascript on the browser). Click on the eye icon in the top right corner of the app to supply the decryption password and .passkey file.


## CLI Reference

```
$ java -jar navopt-workload-anonymizer-0.1-SNAPSHOT.jar 
*********************************
ANON-6 : Missing required option: i
Issues parsing arguments
*********************************
usage:


               java -jar navopt-workload-anonymizer-0.1-SNAPSHOT.jar


This is the Anonymizer utility for Cloudera Navigator Optimizer.
It is used to anonymize column and table names in sql workload. It can also be used to mask literals in the queries.
It can process csv files or semi-colon seprated sql files.

 -a,--anonymize                                             Anonymize or encrypt the queries. If both -a and -d are
                                                            absent, module will default to anonymization.
 -d,--deanonymize                                           Deanonymize or decrypt the queries.
 -h,--header_row <Number of Header rows>                    If processing csv, number of header rows to be ignored from
                                                            processing, Defaults to 1
 -i,--input <FILE CONTAINING QUERIES>                       Input file path
 -k,--key <SECRET FILE>                                     Key file name, Defaults to <input file name>.passkey. This
                                                            is a secret key generated during encryption and need to be
                                                            passed during decryption.
 -l,--skip_mask_literals                                    If literals have to be mask. Literals once masked can not be
                                                            un masked.
 -o,--output <OUTPUT FILE, WILL BE WRITTEN>                 Output file name, Defaults to anon_<input file name> or
                                                            deanon_<input file name>
 -q,--query_col_number <Column number which holds query.>   If csv is passed the column number which has the sql query,
                                                            counting starts from 1.
 -sql                                                       Pass if the input is a sql file, to be split on ;
 -t,--skip_anonymize_table_columns                          If table names and column names have to be anonymized
```

## Troubleshooting

1. Hive and Impala queries are currently not supported 
2. Cannot anonymize alias when using the older aliasing syntax from Teradata.
```
SELECT sample_column (NAMED aliasName) from t1
```
Here aliasName cannot be encrypted

## FAQs

#### What safeguards are present to ensure the integrity of the NavOpt product?
>NavOpt is working towards industry-standard SOC-2, HIPAA HITECH, ISO27001 certification. NavOpt processes and practices are periodically audited by independent auditors, and all code check-ins go through rigorous code and security reviews. Strict change-management processes are adhered to.

#### What safeguards are present to ensure the security of the NavOpt cloud environment?
>NavOpt uses security best-practices in line with SOC-2, HIPAA HITECH, and ISO27001 standards. For example, all data in databases are encrypted, and the cloud environment is routinely scanned for vulnerabilities. 

#### How secure is the encryption?
>NavOpt uses AES-128, one of the most secure encryption algorithms available today. Cracking a 128 bit AES key with a state-of-the-art supercomputer would take longer than the presumed age of the universe. As of today, no practicable attack against AES exists. Therefore, AES remains the preferred encryption standard for governments, banks, and high security systems around the world.

