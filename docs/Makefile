all:	fetch.pdf fetch-One-time-run.pdf

fetch.pdf:	fetch.dot
	dot -Tpdf fetch.dot -o fetch.pdf

fetch-One-time-run.pdf: fetch_one_time_run_sequence.wsdia
	echo "use the website https://www.websequencediagrams.com/ to produce the fetch-One-time-run.pdf file"

