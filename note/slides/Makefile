MARP    ?= npx @marp-team/marp-cli
SRC     := seaweedfs-production-setup.md
PDF     := $(SRC:.md=.pdf)
HTML    := $(SRC:.md=.html)
PPTX    := $(SRC:.md=.pptx)
FLAGS   := --allow-local-files

.PHONY: all pdf html pptx watch clean

all: pdf

pdf: $(PDF)

html: $(HTML)

pptx: $(PPTX)

$(PDF): $(SRC)
	$(MARP) $(SRC) --pdf $(FLAGS) -o $@

$(HTML): $(SRC)
	$(MARP) $(SRC) --html $(FLAGS) -o $@

$(PPTX): $(SRC)
	$(MARP) $(SRC) --pptx $(FLAGS) -o $@

watch:
	$(MARP) $(SRC) --watch --html $(FLAGS)

clean:
	rm -f $(PDF) $(HTML) $(PPTX)
