# DataMosaic: Document-to-Database Extraction Evaluation

This directory contains the evaluation code and datasets for the Doc2DB (Document-to-Database) system presented in our paper.

## Overview

DataMosaic is an evaluation framework for assessing document extraction and database population performance across multiple methods and LLM models. It tests the system's ability to extract structured information from documents and populate database tables.

## Directory Structure

```
DataMosaic/
├── test_dataset_evaluate.py    # Full system evaluation script
├── llm_evaluate.py              # Direct LLM extraction evaluation
├── modules/                     # Core system modules
│   ├── agents/                  # Agent implementations (orchestrator, verifier, etc.)
│   ├── core/                    # Core functionality
│   ├── memory/                  # Memory management
│   ├── services/                # Service layer
│   ├── signals/                 # Signal handling
│   └── utils/                   # Utility functions
└── dataset/                     # Evaluation datasets
    ├── FinanceDB/               # Financial domain dataset
    ├── LegalDB/                 # Legal domain dataset
    ├── PaperDB/                 # Academic paper dataset
    └── StudentDB/               # Student information dataset
```

## Evaluation Scripts

### 1. Full System Evaluation (`test_dataset_evaluate.py`)

Evaluates the complete Doc2DB pipeline with various configurations:

```bash
python test_dataset_evaluate.py --dataset FinanceDB --case case1 --model gpt-4o
```

**Key Parameters:**
- `--dataset`: Dataset to evaluate (FinanceDB, PaperDB, StudentDB, LegalDB)
- `--case`: Test case within the dataset
- `--model`: LLM model to use (gpt-4o, claude-3.5-sonnet, qwen2.5-14b, qwen2.5-72b, etc.)
- `--run_name`: Custom run identifier for output

### 2. LLM-only Evaluation (`llm_evaluate.py`)

Directly tests LLM extraction capabilities without the full pipeline:

```bash
python llm_evaluate.py --dataset FinanceDB --case case1 --model gpt-4o
```

This script bypasses the complete system and evaluates raw LLM performance on extraction tasks.

## Datasets

Four domain-specific datasets are included:

- **FinanceDB**: Financial documents with 5 test cases
- **PaperDB**: Academic papers with 2 test cases
- **StudentDB**: Student records with 1 test case
- **LegalDB**: Legal documents with 1 test case

Each dataset contains:
- Source documents
- Database schemas
- Ground truth annotations
- Test case configurations

## Evaluation Metrics

The evaluation produces four metrics:
- **P (Precision)**: Accuracy of extracted information
- **R (Recall)**: Completeness of extraction
- **F1**: Harmonic mean of precision and recall
- **LLM Score**: Quality assessment by LLM evaluator (0-100)

## Results

Detailed evaluation results comparing different methods can be found in `results.md`. The results show performance across:
- Multiple LLM models (GPT-4o, Claude-3.5-Sonnet, Qwen2.5-14B/72B)
- System variants (with/without `DataMosaic` enhancements)
- Baseline methods (LangChain, LangExtract, EAE)

Example results for FinanceDB case1:

| Method | P | R | F1 | LLM Score |
|--------|-------|-------|--------|-----------|
| qwen2.5-14b + DataMosaic | 22.88 | 71.37 | 34.65 | 65.00 |
| langchain | 20.85 | 61.11 | 31.09 | 45.00 |
| gpt-4o | 22.02 | 52.14 | 30.96 | 40.00 |

See `results.md` for complete results across all datasets and test cases.

## Requirements

The system requires:
- Python 3.8+
- LLM API access (OpenAI, Anthropic, or local deployment)
- Environment configuration in `../llm/.env`

Key dependencies are managed through the parent project's requirements.

## Usage Notes

- Ensure the backend service is running on `localhost:5000` for full system evaluation
- Configure API keys and endpoints in the `.env` file
- Output results are saved to `../dataset_output/` directory
- Evaluation can take significant time depending on dataset size and LLM speed