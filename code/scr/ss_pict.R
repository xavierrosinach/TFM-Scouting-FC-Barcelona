library(curl)

args <- commandArgs(trailingOnly = TRUE)
image_url <- args[1]
output_path <- args[2]

h <- new_handle()
handle_setheaders(h, "User-Agent" = "Mozilla/5.0", "Referer" = "https://www.sofascore.com/")

bin <- curl_fetch_memory(image_url, handle = h)$content
writeBin(bin, output_path)