# CreamSkimming

This is the repository for the [CreamSkimming](https://resume.fmorenovr.com/documents/papers/conferences/2023_CSR.pdf) paper.  
Here we analyze the [CrimeBB dataset](https://www.repository.cam.ac.uk/items/6f20f707-d52f-4655-b944-5da5ef8b98ba) to classify textual information into exploitation types.

# Requirements

- **Python**>=3.12

## Overview
CreamSkimming perform Natural Language Processing (NLP) to select, clean, and filter relevant information related to Common Vulnerabilities and Exposures (CVE). Applying a text classification model we predict the level of threat discussed within messages in dark forums.

### Dataset
The CrimeBB dataset is one of the largest publicly available collections of cybercrime forum data. It was created to support research in cybersecurity, cybercrime analysis, threat intelligence, and NLP—particularly tasks like user behavior analysis, fraud detection, and underground market dynamics.

### Download
To download the dataset, use [this link](https://www.cambridgecybercrime.uk/process.html). Or send an e-mail asking for data.

## Data Preparation

* Clone this repository:

  ```bash
  git clone https://github.com/famveer/CreamSkimming
  git submodule add -b main https://github.com/fmorenovr/nlpToolkit.git py/nlpToolkit
  git submodule update --remote
  ```

* Download dataset [here]().  
* Create a `.env` file, and add the path of the data downloaded and models.  
  ```
    DATA_PATH=/path_to/datasets/
    MODEL_PATH=/path_to/models/
    USER_PASSWD=admin_pass
  ```
  The admin pass is used to create and give dir permissions to create and update files.
  
* First, run the notebook `notebooks/SQL/Extracting_Zip.ipynb`.  
  Then, execute `notebooks/SQL/SQL_backup.ipynb`
  and `notebooks/SQL/SQL_to_CSV.ipynb`
  
* Then, run notebooks `notebooks/Data/Preprocessing.ipynb` and  
  `notebooks/Data/Statistics.ipynb`.

* Next, process the relevant date in notebooks `notebooks/HackForums/CVE_codes.ipynb`  
  Then, `notebooks/HackForums/Labels.ipynb`, `notebooks/HackForums/Languages.ipynb` and `notebooks/HackForums/CVE_Statistics.ipynb`  
  
* Perform feature extraction in `notebooks/Features/Extraction.ipynb`  

* Finally, train and see results in `notebooks/Models/Linear_and_Ensemble_Models.ipynb`  
  
  
# Citation
If you use this data, please cite:

```
@inproceedings{moreno2023cream,
  title={Cream Skimming the Underground: Identifying Relevant Information Points from Online Forums},
  author={Moreno-Vera, Felipe and Nogueira, Mateus and Figueiredo, Cain{\~a} and Menasch{\'e}, Daniel S and Bicudo, Miguel and Woiwood, Ashton and Lovat, Enrico and Kocheturov, Anton and de Aguiar, Leandro Pfleger},
  booktitle={2023 IEEE International Conference on Cyber Security and Resilience (CSR)},
  pages={66--71},
  year={2023},
  organization={IEEE}
}
```

# Contact us  
For any issue please kindly email to `felipe [dot] moreno [at] ppgi [dot] ufrj [dot] br`
