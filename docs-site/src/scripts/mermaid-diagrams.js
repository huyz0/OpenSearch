import mermaid from 'mermaid';
import svgPanZoom from 'svg-pan-zoom';

const RENDERED_MARKER = 'data-mermaid-rendered';
let renderSeq = 0;
let activeLightbox = null;

function currentTheme() {
	const attr = document.documentElement.getAttribute('data-theme');
	return attr === 'dark' ? 'dark' : 'default';
}

function extractSource(preEl) {
	const lines = preEl.querySelectorAll('code > .ec-line');
	if (lines.length > 0) {
		return Array.from(lines)
			.map((line) => line.textContent ?? '')
			.join('\n');
	}
	// Fallback if Expressive Code's line-wrapping markup isn't present.
	return preEl.textContent ?? '';
}

function closeLightbox() {
	if (!activeLightbox) return;
	activeLightbox.panZoom.destroy();
	activeLightbox.overlay.remove();
	document.removeEventListener('keydown', activeLightbox.onKeydown);
	activeLightbox = null;
}

function openLightbox(svgEl) {
	closeLightbox();

	const overlay = document.createElement('div');
	overlay.className = 'mmd-lightbox-overlay';

	const stage = document.createElement('div');
	stage.className = 'mmd-lightbox-stage';
	const clone = svgEl.cloneNode(true);
	clone.removeAttribute('style');
	clone.style.width = '100%';
	clone.style.height = '100%';
	stage.appendChild(clone);

	const hint = document.createElement('div');
	hint.className = 'mmd-lightbox-hint';
	hint.textContent = 'Scroll to zoom, drag to pan — click outside or press Esc to close';

	overlay.append(stage, hint);
	document.body.appendChild(overlay);

	// requestAnimationFrame so the element has real layout dimensions before svg-pan-zoom measures it.
	requestAnimationFrame(() => {
		const panZoom = svgPanZoom(clone, {
			zoomEnabled: true,
			panEnabled: true,
			controlIconsEnabled: true,
			fit: true,
			center: true,
			minZoom: 0.5,
			maxZoom: 20,
		});

		const onKeydown = (e) => {
			if (e.key === 'Escape') closeLightbox();
		};
		document.addEventListener('keydown', onKeydown);

		overlay.addEventListener('click', (e) => {
			if (e.target === overlay || e.target === hint) closeLightbox();
		});

		activeLightbox = { overlay, panZoom, onKeydown };
	});
}

async function renderOne(preEl, theme) {
	const source = extractSource(preEl).trim();
	if (!source) return;

	const wrapper = document.createElement('div');
	wrapper.className = 'mmd-wrapper not-content';
	wrapper.title = 'Click to zoom';

	const sourceHolder = document.createElement('span');
	sourceHolder.className = 'mmd-source-holder';
	sourceHolder.hidden = true;
	sourceHolder.textContent = source;
	wrapper.appendChild(sourceHolder);

	const host = preEl.closest('.expressive-code') ?? preEl;
	host.replaceWith(wrapper);

	try {
		renderSeq += 1;
		const id = `mmd-diagram-${renderSeq}`;
		mermaid.initialize({ startOnLoad: false, theme, securityLevel: 'strict' });
		const { svg } = await mermaid.render(id, source);
		wrapper.insertAdjacentHTML('afterbegin', svg);
		const svgEl = wrapper.querySelector('svg');
		if (svgEl) {
			svgEl.removeAttribute('height');
			svgEl.style.maxWidth = '100%';
			wrapper.addEventListener('click', () => openLightbox(svgEl));
		}
	} catch (err) {
		wrapper.replaceWith(host); // restore original code block on render failure
		console.error('mermaid render failed', err);
	}
}

function renderAll() {
	const blocks = document.querySelectorAll(`pre[data-language="mermaid"]:not([${RENDERED_MARKER}])`);
	if (blocks.length === 0) return;
	const theme = currentTheme();
	blocks.forEach((block) => {
		block.setAttribute(RENDERED_MARKER, '1');
		renderOne(block, theme);
	});
}

function rerenderAllForThemeChange() {
	const theme = currentTheme();
	document.querySelectorAll('.mmd-wrapper').forEach((wrapper) => {
		const sourceHolder = wrapper.querySelector('.mmd-source-holder');
		const oldSvg = wrapper.querySelector('svg');
		if (!sourceHolder || !oldSvg) return;
		renderSeq += 1;
		const id = `mmd-diagram-${renderSeq}`;
		mermaid.initialize({ startOnLoad: false, theme, securityLevel: 'strict' });
		mermaid.render(id, sourceHolder.textContent ?? '').then(({ svg }) => {
			oldSvg.outerHTML = svg;
			const svgEl = wrapper.querySelector('svg');
			if (svgEl) {
				svgEl.removeAttribute('height');
				svgEl.style.maxWidth = '100%';
			}
		});
	});
}

function init() {
	renderAll();

	const observer = new MutationObserver((mutations) => {
		for (const m of mutations) {
			if (m.attributeName === 'data-theme') {
				rerenderAllForThemeChange();
			}
		}
	});
	observer.observe(document.documentElement, { attributes: true });
}

// astro:page-load fires on every navigation when View Transitions are active; on a plain
// full page load (no ClientRouter) it never fires at all, so we also run on ordinary
// DOM-ready. Both listeners can end up calling renderAll() — it's idempotent via the
// RENDERED_MARKER attribute, so a double-invocation is harmless.
document.addEventListener('astro:page-load', init);
if (document.readyState === 'loading') {
	document.addEventListener('DOMContentLoaded', init);
} else {
	init();
}
