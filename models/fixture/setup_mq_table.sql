CREATE TABLE IF NOT EXISTS `pubsub` (
  `id` varchar(255) NOT NULL,
  `value` blob NOT NULL,
  PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;
DELETE FROM `pubsub`;
