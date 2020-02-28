# JellyDB
A database management module for Python inspired by [L-Store](https://www.researchgate.net/publication/324150481_L-Store_A_Real-time_OLTP_and_OLAP_System).

## Usage

To run Milestone 2 testers (correctness test cases from instructors)
```
mkdir ~/ECS165
python -m JellyDB.m2_tester_part1
python -m JellyDB.m2_tester_part2
```

To run Milestone 2 index tester (non-primary key select test cases from instructors)
```
rm ~/ECS165/*
python -m JellyDB.index_tester
```

To run `test_on_correctness.py` (correctness test cases by JellyDB team):
```
python -m JellyDB.test_on_correctness
```
To run `performance_m2.py` (performance test cases by JellyDB team; loops to test a parameter 10 times, expect minimum 10 min to complete per round)
```
python performance_m2.py
```

## Team
- __Benjamin Rausch__: Systems architect
- __Lisa Malins__: Developer, correctness testing
- __Jessica LaCourse__: Developer, live demo notebook
- __Yinuo Zhang__: Developer, performance testing
- __Felicity Meng__: Developer, performance testing, and slides

## References
Sadoghi M, Bhattacherjee S, Bhattacharjee B, Canim M. L-store: A real-time OLTP and OLAP system. arXiv preprint arXiv:1601.04084. 2016 Jan 15.
