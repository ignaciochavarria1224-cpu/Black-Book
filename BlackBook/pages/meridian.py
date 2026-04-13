"""
pages/meridian.py — Meridian belief graph and wiki themes.
Uses vis-network via injected CDN script + hidden JSON data bridge.
"""
from __future__ import annotations

import reflex as rx

from BlackBook.state.meridian_state import MeridianState


def theme_chip(t: dict) -> rx.Component:
    return rx.el.button(
        t["theme"],
        class_name=rx.cond(
            MeridianState.selected_theme == t["theme"],
            "bb-tag",
            "bb-btn bb-btn-ghost",
        ),
        on_click=MeridianState.select_theme(t["theme"]),
        style={"font_size": "0.52rem", "padding": "0.2rem 0.55rem", "margin": "0.15rem"},
    )


def meridian_page() -> rx.Component:
    return rx.fragment(
        rx.el.div(
            rx.el.h1("Meridian", class_name="bb-title"),
            rx.el.p("BELIEF GRAPH · WIKI THEMES · COGNITIVE MAP", class_name="bb-subtitle"),
        ),

        # Stats row
        rx.el.div(
            rx.el.div(
                rx.el.div("Themes", class_name="bb-stat-label"),
                rx.el.div(MeridianState.theme_count, class_name="bb-stat-value accent"),
                class_name="bb-stat",
                style={"max_width": "160px"},
            ),
            style={"display": "flex", "gap": "1rem", "margin_bottom": "1.5rem"},
        ),

        # Search
        rx.el.input(
            placeholder="Search themes...",
            value=MeridianState.search,
            on_change=MeridianState.set_search,
            class_name="bb-input",
            style={"max_width": "340px", "margin_bottom": "1.2rem"},
        ),

        # Two-column layout: graph + detail
        rx.el.div(
            # Graph canvas
            rx.el.div(
                rx.el.div(
                    id="meridian-graph",
                    style={"width": "100%", "height": "520px"},
                ),
                class_name="bb-graph-wrap",
                style={"flex": "1.4"},
            ),
            # Detail panel
            rx.el.div(
                rx.cond(
                    MeridianState.selected_theme != "",
                    rx.el.div(
                        rx.el.div(MeridianState.selected_theme, class_name="bb-theme-name"),
                        rx.el.div(MeridianState.selected_body, class_name="bb-theme-body"),
                    ),
                    rx.el.div(
                        "Click a node or theme chip to explore.",
                        style={"color": "var(--t2)", "font_size": "0.78rem"},
                    ),
                ),
                class_name="bb-theme-detail",
                style={"flex": "1"},
            ),
            style={"display": "flex", "gap": "1.2rem", "align_items": "flex-start"},
        ),

        # Theme chips list
        rx.el.div("All Themes", class_name="bb-section"),
        rx.el.div(
            rx.foreach(MeridianState.filtered_themes, theme_chip),
            style={"display": "flex", "flex_wrap": "wrap", "gap": "0.25rem"},
        ),

        # Graph data + vis-network initialization
        # We inject a script that reads state vars serialized to data attributes on a hidden div
        rx.el.div(
            id="meridian-data",
            data_nodes=MeridianState.graph_nodes_json,
            data_edges=MeridianState.graph_edges_json,
            style={"display": "none"},
        ),
        rx.html(
            '<script src="https://unpkg.com/vis-network@9.1.9/standalone/umd/vis-network.min.js"></script>'
        ),
        rx.html("""
<script>
(function() {
  var _net = null;

  function buildGraph() {
    var dataEl = document.getElementById('meridian-data');
    if (!dataEl) return;
    var nodesRaw = dataEl.getAttribute('data-nodes') || '[]';
    var edgesRaw = dataEl.getAttribute('data-edges') || '[]';
    var nodes, edges;
    try { nodes = JSON.parse(nodesRaw); edges = JSON.parse(edgesRaw); }
    catch(e) { return; }
    if (!nodes.length) return;

    var container = document.getElementById('meridian-graph');
    if (!container) return;

    var palette = ['#E040FB','#BD34FE','#00E5FF','#00FFB3'];
    var nodeData = nodes.map(function(n, i) {
      return {
        id: n.id,
        label: n.label,
        color: {
          background: palette[i % palette.length],
          border: 'rgba(255,255,255,0.12)',
          highlight: { background: '#ffffff', border: '#E040FB' }
        },
        font: { color: '#F0EBFFeb', size: 11, face: 'JetBrains Mono' },
        borderWidth: 1,
        shape: 'dot',
        size: 10 + Math.min((n.cycle || 0), 5) * 2
      };
    });
    var edgeData = edges.map(function(e) {
      return {
        from: e.from, to: e.to,
        color: { color: 'rgba(189,52,254,0.35)', highlight: '#E040FB' },
        width: 1,
        smooth: { type: 'curvedCW', roundness: 0.2 },
        arrows: { to: { enabled: true, scaleFactor: 0.5 } }
      };
    });

    if (_net) { _net.destroy(); _net = null; }
    var data = {
      nodes: new vis.DataSet(nodeData),
      edges: new vis.DataSet(edgeData)
    };
    var options = {
      physics: {
        enabled: true,
        barnesHut: { gravitationalConstant: -8000, springLength: 130, damping: 0.15 },
        stabilization: { iterations: 100 }
      },
      interaction: { hover: true, zoomView: true, dragView: true },
      layout: { improvedLayout: true }
    };

    _net = new vis.Network(container, data, options);
    _net.on('selectNode', function(params) {
      if (!params.nodes.length) return;
      var nodeId = params.nodes[0];
      // Highlight the matching chip
      document.querySelectorAll('.bb-nav-item').forEach(function(el) {
        el.style.color = '';
      });
    });
  }

  // Watch for data attribute changes (Reflex updates DOM)
  var observer = new MutationObserver(function() { buildGraph(); });
  function attachObserver() {
    var el = document.getElementById('meridian-data');
    if (el) {
      observer.observe(el, { attributes: true });
      buildGraph();
    } else {
      setTimeout(attachObserver, 300);
    }
  }
  setTimeout(attachObserver, 500);
})();
</script>
"""),

        on_mount=MeridianState.load,
    )
