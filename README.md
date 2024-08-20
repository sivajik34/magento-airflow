# Magento- Apache Airflow
---

### **Airflow Magento Provider**

This repository contains a custom Apache Airflow provider for integrating with Magento 2. The provider enables the automation of various Magento-related tasks using Airflow, such as order synchronization and data exports.

#### **Features:**
- **Magento Integration**: Seamlessly connect to Magento 2 using OAuth1 authentication for secure API requests.  
- **Custom Airflow Hooks**: Simplifies interaction with Magento's REST API.
- **Custom Airflow Operators**: Provides an operator to fetch and manipulate orders and other data from Magento.

#### **Contents:**
- **`airflow_magento_provider/hooks/magento.py`**: Contains the `MagentoHook` class, which handles OAuth authentication and API requests to Magento.
- **`airflow_magento_provider/operators/sales.py`**: Contains the `GetOrdersOperator` class, which fetches orders from Magento and allows for further processing.
- **`example_dags/magento_order_sync.py`**: Example DAG for synchronizing products from Magento and exporting them to a CSV file.

#### **Getting Started:**
1. **Install the provider**:
   Clone the repository and install the package using pip:
   ```bash
   git clone [https://github.com/your-username/airflow-magento-provider.git](https://github.com/sivajik34/magento-airflow.git)
   cd apache_airflow_provider_magento
   pip install .
   ```

2. **Set up Airflow connections**:
   - Create an Airflow connection with your Magento credentials( System -> Integration -> Add new Integration).

3. **Create a DAG**:
   - Set up your order synchronization pipeline for the example DAG in `example_dags/magento_order_sync.py`.

#### **Requirements:**
- Apache Airflow 2.x
- Magento 2.x
- Python 3.8+

#### **Contributing:**
Contributions are welcome! Please feel free to submit a pull request or open an issue.

#### **License:**
This project is licensed under the Opensource License.

---
