CREATE TABLE 5118_data(
  id int PRIMARY KEY AUTO_INCREMENT,
  co_id int,
  word VARCHAR(80),
  bd_index int,
  result int,
  bid_co int,
  pc VARCHAR(20),
  mobile VARCHAR(20),
  bid_level int,
  UNIQUE KEY word_key (word(80))
)ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE 5118_co(
  id int PRIMARY KEY AUTO_INCREMENT,
  co_id int,
  co_name VARCHAR(80),
  amount int,
  UNIQUE KEY co_id_key (co_id)
)ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;