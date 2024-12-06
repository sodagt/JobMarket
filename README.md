# JobMarket
Projet datascientest

## **Dockerfile:**


1. **API**

<p> 
This configuration sets up a Docker container that performs initial setup tasks for an Elasticsearch cluster, including creating a Certificate Authority (CA), generating certificates, setting file permissions, and configuring passwords. Here's a breakdown of its functionality and purpose:

- **Image and Volume:**
<br>
image: docker.elastic.co/elasticsearch/elasticsearch:${STACK_VERSION}: Specifies the Elasticsearch Docker image, with the version defined by the STACK_VERSION environment variable.
volumes: Mounts a volume to store generated certificates (certs:/usr/share/elasticsearch/config/certs).

- **User:**
<br>
user: "0": Ensures the container runs as the root user, required for setting file permissions during certificate generation.

- **Command: The main bash script performs the following:** 
<p> 

    Password Validation:
    Ensures that ELASTIC_PASSWORD (for the elastic user) and KIBANA_PASSWORD (for the kibana_system user) are set in the .env file. If not, the script exits with an error.

    Certificate Authority (CA) Creation:
    Checks if certs/ca.zip exists. If not, it creates a CA using elasticsearch-certutil.
    Extracts the generated CA files into the certs directory.

    Certificate Creation:
    Checks if certs/certs.zip exists. If not, it creates node-specific certificates for es01, es02, and es03.
    The instances.yml file lists the nodes, their DNS names, and IPs for certificate generation.

    Set Permissions:
    Adjusts ownership and permissions for all files and directories in certs to ensure secure access.

    Cluster Availability Check:
    Polls the Elasticsearch cluster (es01) for availability using curl and waits until it responds.

    Set kibana_system Password:
    Updates the password for the kibana_system user by sending a POST request to the Elasticsearch security API.
</p> 


- **Healthcheck:**
<br>
Purpose: Validates whether the setup process succeeded.
Test: Checks if the certificate file for es01 (config/certs/es01/es01.crt) exists.
Retries: Tries up to 120 times (1-second interval, 5-second timeout).

<br>
<br>

2. **ES/KIBANA**

- **Node Name and Cluster Configuration:** 
<br>
node.name=es03: Assigns a unique name to the node within the cluster.
cluster.name=${CLUSTER_NAME}: Specifies the name of the Elasticsearch cluster. All nodes in the same cluster must have the same cluster name.
cluster.initial_master_nodes=es01,es02,es03: Lists the initial set of nodes eligible to become master nodes. This is crucial for cluster bootstrapping.
discovery.seed_hosts=es01,es02: Specifies the hostnames or IPs of nodes to contact during cluster formation.

- **Memory and Performance Settings:**
<br>
bootstrap.memory_lock=true: Prevents the JVM from swapping memory to disk, improving performance and stability.
"ES_JAVA_OPTS=-Xms256m -Xmx256m": Sets the minimum (-Xms) and maximum (-Xmx) heap size for the JVM to 256 MB. Adjust based on available resources.

- **Security Configuration:**
<br>
xpack.security.enabled=true: Enables Elasticsearch's built-in security features (e.g., authentication, authorization, TLS).
xpack.monitoring.collection.enabled=true: Enables collection of monitoring data for the node.

- **SSL/TLS for HTTP:**
<br>
xpack.security.http.ssl.enabled=true: Enables SSL/TLS for HTTP communications.
xpack.security.http.ssl.key=certs/es03/es03.key: Specifies the private key for the node’s HTTP interface.
xpack.security.http.ssl.certificate=certs/es03/es03.crt: Specifies the certificate for the node’s HTTP interface.
xpack.security.http.ssl.certificate_authorities=certs/ca/ca.crt: Provides the CA certificate to validate the SSL/TLS certificate chain.
xpack.security.http.ssl.verification_mode=certificate: Ensures that only certificates signed by a trusted CA are accepted.

- **SSL/TLS for Transport Layer:**
<br>
xpack.security.transport.ssl.enabled=true: Enables SSL/TLS for transport layer communications (inter-node communication).
xpack.security.transport.ssl.key=certs/es03/es03.key: Specifies the private key for the transport interface.
xpack.security.transport.ssl.certificate=certs/es03/es03.crt: Specifies the certificate for the transport interface.
xpack.security.transport.ssl.certificate_authorities=certs/ca/ca.crt: Provides the CA certificate to validate the SSL/TLS certificate chain for transport.
xpack.security.transport.ssl.verification_mode=certificate: Ensures strict verification of the transport certificates.

- **Licensing:**
<br>
xpack.license.self_generated.type=${LICENSE}: Configures the type of license for X-Pack features (e.g., trial, basic, gold).

</p>