<h1>Google BigQuery CDC writer</h1>

This repo contains:
<ul>
    <li>Google bigquery writer operator</li>
    <li>Google bigquery table creator</li>
    <li>ABAP data transformer</li>
    <li>Protobuf compiler operator</li>
    <li>SLT connector emulators for testing</li>
</ul>

<h3>Setup</h3>

<p>
    To start, we need to ensure our pipeline has the proper dependencies.
    Go to your repo and create a new dockerfile that contains the following code: 

        FROM $com.sap.sles.base

        RUN pip install --user google-cloud-bigquery
        RUN pip install --user google-cloud-bigquery-storage
        RUN pip install --user google-cloud-storage
        RUN pip install --user pyarrow==12.0.1

        ENV PROTOC_VERSION=23.4

        RUN curl -L "https://github.com/protocolbuffers/protobuf/releases/download/v${PROTOC_VERSION}/protoc-${PROTOC_VERSION}-linux-x86_64.zip" -o /home/vflow/protoc.zip 
        RUN unzip /home/vflow/protoc.zip -d /home/vflow 
        RUN rm -rf /tmp/vflow/protoc.zip
    

    Make sure to tag the dockerfile as "bigquery" so our operators can use it later.
</p>

