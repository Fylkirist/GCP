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

<h3>Emulators</h3>

<p>
    The slt emulators have 2 outputs, output and ABAP. Output is the port that sends the complete schema and body.
</p>

<h3>Protobuf compiler<h3>

<p>
    Using the compiler in your pipeline is recommended but not required, it makes starting up a new pipeline or making changes to your table smoother by doing a one time protobuf compilation upon initial load. In order to use it, place it ahead of the CDC writer operator, and set "precompiled" to false. Connect the SLT connector to the protobuf compiler input and connect the output to the CDC writer, the info port will send the proto schema so you can see it on a wiretap.
</p>
<img src="pipeline.PNG">
<br>
<img src="CDCconfig.PNG">

<h3>CDC writer<h3>

<p>
    This operator writes to Google bigquery.
    To use, you need to select your credentials, then enter the names of your dataset and table you want to update.
    If you are manually compiling your protobufs, set "precompiled" to true, and substitute the contents of buffer_dict in the manner described below:
</p>
<p>
    Your message name must be TUNIT in your proto file.
    Copy the following from your compiled pb2 file:
</p>
<img src="copythis.PNG">
<p>
    Create a new dictionary entry using the source table name in buffer_dict like so:
</p>
 ```py

    buffer_dict = {
        "SOURCETABLE":{
            "bytes":b"your_bytestring_here",
            "start":0,
            "end":0,
            "options":None,
            "topmessage":"message_protobuf_name"
        }
    }

```
