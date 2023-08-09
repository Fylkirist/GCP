FROM $com.sap.sles.base

RUN pip install --user google-cloud-bigquery
RUN pip install --user google-cloud-bigquery-storage
RUN pip install --user google-cloud-storage
RUN pip install --user pyarrow==12.0.1

ENV PROTOC_VERSION=23.4

RUN curl -L "https://github.com/protocolbuffers/protobuf/releases/download/v${PROTOC_VERSION}/protoc-${PROTOC_VERSION}-linux-x86_64.zip" -o /home/vflow/protoc.zip 
RUN unzip /home/vflow/protoc.zip -d /home/vflow 
RUN rm -rf /tmp/vflow/protoc.zip

RUN curl  -L "https://raw.githubusercontent.com/Fylkirist/data-intelligence-slt-to-biqquery-cdc/main/testing/ABAPmeta.py" -O
