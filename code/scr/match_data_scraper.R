library(httr)

args <- commandArgs(trailingOnly = TRUE)
match_id <- args[1]
out_path <- args[2]

match_link <- paste0("https://www.fotmob.com/api/matchDetails?matchId=", match_id)
json_txt <- content(GET(match_link), as = "text", encoding = "UTF-8")

writeLines(json_txt, out_path)
