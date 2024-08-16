# Magento- Apache Airflow
---

### **Airflow Magento Provider**

This repository contains a custom Apache Airflow provider for integrating with Magento 2. The provider enables the automation of various Magento-related tasks using Airflow, such as product synchronization and data exports.

#### **Features:**
- **Magento Integration**: Seamlessly connect to Magento 2 using OAuth1 authentication for secure API requests.
- **Product Synchronization**: Fetch and sync product data from Magento and export it to CSV files.
- **Custom Airflow Hooks**: Simplifies interaction with Magento's REST API.
- **Custom Airflow Operators**: Provides an operator to fetch and manipulate product data from Magento.

#### **Contents:**
- **`airflow_magento_provider/hooks/magento_hooks.py`**: Contains the `MagentoHook` class, which handles OAuth authentication and API requests to Magento.
- **`airflow_magento_provider/operators/product_sync_operator.py`**: Contains the `ProductSyncOperator` class, which fetches products from Magento and allows for further processing.
- **`dags/magento_product_sync.py`**: Example DAG for synchronizing products from Magento and exporting them to a CSV file.

#### **Getting Started:**
1. **Install the provider**:
   Clone the repository and install the package using pip:
   ```bash
   git clone https://github.com/your-username/airflow-magento-provider.git
   cd src/airflow-magento-provider
   pip install .
   ```

2. **Set up Airflow connections**:
   - Create an Airflow connection with your Magento credentials.

3. **Create a DAG**:
   - Use the example DAG provided in `dags/magento_product_sync.py` to set up your own product synchronization pipeline.

#### **Requirements:**
- Apache Airflow 2.x
- Magento 2.x
- Python 3.6+

#### **Contributing:**
Contributions are welcome! Please feel free to submit a pull request or open an issue.

#### **License:**
This project is licensed under the Opensource License.

---
