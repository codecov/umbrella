from graphs.helpers.graph_utils import _svg_rect, _tree_height


class TestGraphsUtils:
    def test_tree_height(self):
        tree = [{"name": "name_0"}]

        height = _tree_height(tree)
        assert height == 1

        tree = [{"name": "name_0", "children": [{"name": "name_2"}]}]

        height = _tree_height(tree)
        assert height == 2

        tree = [
            {
                "name": "name_0",
                "children": [
                    {"name": "name_1", "children": [{"name": "name_2"}]},
                    {"name": "name_3"},
                ],
            }
        ]
        height = _tree_height(tree)
        assert height == 3

        tree = [
            {
                "name": "name_0",
                "children": [
                    {
                        "name": "name_1",
                        "children": [
                            {"name": "name_2", "children": [{"name": "name_4"}]}
                        ],
                    },
                    {"name": "name_3"},
                ],
            }
        ]
        height = _tree_height(tree)
        assert height == 4

    def test_svg_rect_without_title(self):
        """Test that _svg_rect without title works correctly"""
        result = _svg_rect(
            x=10, y=20, width=100, height=50, fill="#fff", stroke="#000", stroke_width=1
        )

        assert '<rect x="10" y="20" width="100" height="50"' in result
        assert 'fill="#fff"' in result
        assert 'stroke="#000"' in result
        assert 'stroke-width="1"' in result
        assert "<title>" not in result

    def test_svg_rect_with_normal_title(self):
        """Test that _svg_rect with normal title includes title and data-content"""
        result = _svg_rect(
            x=10,
            y=20,
            width=100,
            height=50,
            fill="#fff",
            stroke="#000",
            stroke_width=1,
            title="path/to/file.py",
        )

        assert '<rect x="10" y="20" width="100" height="50"' in result
        assert 'data-content="path/to/file.py"' in result
        assert "<title>path/to/file.py</title>" in result
        assert "tooltipped" in result

    def test_svg_rect_escapes_xss_script_tags(self):
        """Test that script tags in file paths are properly escaped"""
        malicious_title = '"><script>alert("XSS")</script><path foo="'
        result = _svg_rect(
            x=10,
            y=20,
            width=100,
            height=50,
            fill="#fff",
            stroke="#000",
            stroke_width=1,
            title=malicious_title,
        )

        # Should not contain unescaped dangerous sequences that would break out of attributes
        assert '"><script>' not in result
        assert "</script>" not in result

        # Should contain escaped version of angle brackets and quotes
        assert "&lt;script&gt;" in result or "&#x3C;script&#x3E;" in result
        assert "&quot;" in result or "&#x27;" in result

    def test_svg_rect_escapes_xss_event_handlers(self):
        """Test that event handlers in file paths are properly escaped"""
        malicious_title = '" onload="alert(\'XSS\')" data="'
        result = _svg_rect(
            x=10,
            y=20,
            width=100,
            height=50,
            fill="#fff",
            stroke="#000",
            stroke_width=1,
            title=malicious_title,
        )

        # Should not contain raw event handlers
        assert 'onload="alert' not in result

        # Should contain escaped quotes
        assert "&quot;" in result or "&#x22;" in result

    def test_svg_rect_escapes_xss_html_injection(self):
        """Test that HTML injection attempts in file paths are properly escaped"""
        malicious_title = "</rect><text>INJECTED</text><rect"
        result = _svg_rect(
            x=10,
            y=20,
            width=100,
            height=50,
            fill="#fff",
            stroke="#000",
            stroke_width=1,
            title=malicious_title,
        )

        # Should not contain unescaped closing tags that would break out of attributes
        assert "</rect><text>INJECTED</text><rect" not in result

        # Should contain escaped version
        assert "&lt;" in result or "&#x3C;" in result
        assert "&gt;" in result or "&#x3E;" in result

    def test_svg_rect_escapes_all_dangerous_chars(self):
        """Test that all dangerous characters are escaped"""
        dangerous_title = "& < > \" ' test"
        result = _svg_rect(
            x=10,
            y=20,
            width=100,
            height=50,
            fill="#fff",
            stroke="#000",
            stroke_width=1,
            title=dangerous_title,
        )

        # Check that dangerous characters are escaped
        # Note: Python's html.escape with quote=True escapes these as:
        # & -> &amp;
        # < -> &lt;
        # > -> &gt;
        # " -> &quot;
        # ' -> &#x27;
        assert "&amp;" in result
        assert "&lt;" in result
        assert "&gt;" in result
        assert "&quot;" in result
        assert "&#x27;" in result

        # Should not contain raw dangerous characters in attribute values
        assert 'data-content="&' in result
        assert 'data-content="<' not in result

    def test_svg_rect_with_realistic_malicious_filename(self):
        """Test with a realistic malicious filename that could exist in a repo"""
        # Simulating a file path like: tests/"><img src=x onerror=alert(1)>.py
        malicious_title = 'tests/"><img src=x onerror=alert(1)>.py'
        result = _svg_rect(
            x=10,
            y=20,
            width=100,
            height=50,
            fill="#fff",
            stroke="#000",
            stroke_width=1,
            title=malicious_title,
        )

        # Should not break out of the attribute with the dangerous sequence
        assert '"><img' not in result
        assert '" onerror=' not in result
        assert "> onerror=" not in result

        # Should be properly escaped
        assert "&quot;&gt;&lt;img" in result or "&#x22;&#x3E;&#x3C;img" in result

    def test_svg_rect_preserves_class_parameter(self):
        """Test that _class parameter works with and without title"""
        # Without title
        result = _svg_rect(
            x=10,
            y=20,
            width=100,
            height=50,
            fill="#fff",
            stroke="#000",
            stroke_width=1,
            _class="my-class",
        )
        assert 'class="my-class"' in result

        # With title
        result_with_title = _svg_rect(
            x=10,
            y=20,
            width=100,
            height=50,
            fill="#fff",
            stroke="#000",
            stroke_width=1,
            _class="my-class",
            title="test/path.py",
        )
        assert 'class="my-class tooltipped"' in result_with_title
