### **Magento Apache Airflow Provider**

This repository contains a custom Apache Airflow provider for integrating with Magento 2. The provider enables the automation of various Magento-related tasks using Airflow, such as order synchronization and data exports.

#### **Features:**
- **Magento Integration**: Seamlessly connect to Magento 2 using OAuth1 authentication for secure API requests.  
- **Custom Airflow Hooks**: Simplifies interaction with Magento's REST API and GraphQL.
- **Custom Airflow Operators**: Provides an operator to fetch and manipulate orders and other data from Magento.

#### **Contents:**
- **`airflow_magento_provider/hooks/magento.py`**: Contains the `MagentoHook` class, which handles OAuth authentication and API requests to Magento.
- **`airflow_magento_provider/operators/rest.py`**: Contains the `MagentoRestOperator` class.
- **`airflow_magento_provider/operators/graphql.py`**: Contains the `MagentoGraphQLOperator` class.
- **`example_dags/`**: Example DAGs for understanding Magento Operators and Hooks.

#### **Getting Started:**
1. **Install the provider**:
   Clone the repository and install the package using pip:
   ```bash
   git clone https://github.com/sivajik34/magento-airflow.git
   cd apache_airflow_provider_magento
   pip install .
   ```

   Also, you can install it from https://pypi.org/project/apache-airflow-provider-magento/
   pip install apache-airflow-provider-magento

3. **Set up Airflow connections**:
   - Create an Airflow connection with your Magento credentials( System -> Integration -> Add new Integration).

4. **Create a DAG**:
   - Set up your data synchronization pipeline by referring  to the example DAGs in the `example_dags` directory.

#### **Requirements:**
- Apache Airflow 2.x
- Magento 2.4.x
- Python 3.8+

#### **Contributing:**
Contributions are welcome! Please feel free to submit a pull request or open an issue.

#### **License:**
This project is licensed under the MIT License.

---
