var  map = Map.empty[String, String]

map += ("France" -> "Paris")

map.contains("France")
map.get("France")
map.get("France1")

map = map.-("France")

map.-("France")