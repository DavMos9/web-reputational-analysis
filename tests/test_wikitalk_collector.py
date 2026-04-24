"""tests/test_wikitalk_collector.py — Unit test per WikiTalkCollector._clean_wikitext."""

from collectors.wikitalk_collector import WikiTalkCollector

clean = WikiTalkCollector._clean_wikitext


class TestCleanWikitextHtmlTags:
    def test_removes_br_tag(self):
        assert "<br>" not in clean("First line.<br>Second line.")
        assert "First line." in clean("First line.<br>Second line.")
        assert "Second line." in clean("First line.<br>Second line.")

    def test_removes_br_self_closing(self):
        assert "<br/>" not in clean("text<br/>more text")
        assert "<br />" not in clean("text<br />more text")

    def test_removes_small_tag_keeps_content(self):
        result = clean("Normal <small>small text</small> normal.")
        assert "<small>" not in result
        assert "</small>" not in result
        assert "small text" in result

    def test_removes_nowiki_tag_keeps_content(self):
        result = clean("See <nowiki>[[link]]</nowiki> here.")
        assert "<nowiki>" not in result
        assert "[[link]]" in result

    def test_removes_ref_tag_with_content(self):
        result = clean("Claim.<ref>Citation text here.</ref> More text.")
        assert "<ref>" not in result
        assert "Citation text here." not in result
        assert "More text." in result

    def test_removes_self_closing_ref(self):
        result = clean("Text<ref name=\"foo\"/> continues.")
        assert "<ref" not in result
        assert "continues." in result

    def test_removes_multiline_ref(self):
        text = "Claim.\n<ref>\nLong\ncitation.\n</ref>\nRest."
        result = clean(text)
        assert "<ref>" not in result
        assert "Long\ncitation." not in result
        assert "Rest." in result

    def test_removes_s_strikethrough_keeps_content(self):
        result = clean("Old <s>wrong info</s> corrected.")
        assert "<s>" not in result
        assert "wrong info" in result

    def test_ref_with_attributes(self):
        result = clean('Text.<ref name="smith2020">Smith 2020, p. 5.</ref> End.')
        assert "<ref" not in result
        assert "Smith 2020" not in result
        assert "End." in result


class TestCleanWikitextTemplates:
    def test_removes_simple_template(self):
        result = clean("Text {{cite web|url=http://x.com}} more.")
        assert "{{" not in result
        assert "}}" not in result

    def test_removes_nested_template(self):
        result = clean("Text {{outer|{{inner|val}}}} more.")
        assert "{{" not in result

    def test_removes_signatures(self):
        result = clean("Comment ~~~~ and more.")
        assert "~~~~" not in result
        assert "~~~" not in result


class TestCleanWikitextLinks:
    def test_wiki_link_with_label(self):
        result = clean("See [[Donald Trump|Trump]] here.")
        assert "[[" not in result
        assert "Trump" in result

    def test_wiki_link_no_label(self):
        result = clean("See [[Donald Trump]] here.")
        assert "[[" not in result
        assert "Donald Trump" in result

    def test_external_link_with_label(self):
        result = clean("See [https://example.com Example site] here.")
        assert "[https://" not in result
        assert "Example site" in result


class TestCleanWikitextSubheadings:
    def test_removes_subheading_markers(self):
        result = clean("=== Sub-section ===\nSome content here.")
        assert "===" not in result
        assert "Some content here." in result

    def test_removes_level2_subheading(self):
        result = clean("== Another section ==\nContent.")
        assert "==" not in result
        assert "Content." in result


class TestCleanWikitextEmpty:
    def test_empty_string(self):
        assert clean("") == ""

    def test_none_equivalent(self):
        # _clean_wikitext with whitespace only returns ""
        assert clean("   \n  ") == ""

    def test_short_text_preserved(self):
        result = clean("Hello world.")
        assert result == "Hello world."
