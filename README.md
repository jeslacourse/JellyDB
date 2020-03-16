# JellyDB
A database management module for Python inspired by [L-Store](https://www.researchgate.net/publication/324150481_L-Store_A_Real-time_OLTP_and_OLAP_System).

## Usage

To run Milestone 3 tester (correctness test cases from instructors):
```
mkdir ~/ECS165
python -m JellyDB.m3_tester
```

To run Milestone 3 transaction tester:
```
rm ~/ECS165/*
python -m JellyDB.transaction_tester
```

For performance testing, we included an R notebook `performance_m3.Rmd`.


## Team
- __Benjamin Rausch__: Systems architect
- __Lisa Malins__: Developer, correctness testing
- __Jessica LaCourse__: Developer, live demo notebook
- __Yinuo Zhang__: Developer, performance testing
- __Felicity Meng__: Developer, performance testing, and slides

## References
Sadoghi M, Bhattacherjee S, Bhattacharjee B, Canim M. L-store: A real-time OLTP and OLAP system. arXiv preprint arXiv:1601.04084. 2016 Jan 15.
