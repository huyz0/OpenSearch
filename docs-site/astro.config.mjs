// @ts-check
import { defineConfig } from 'astro/config';
import starlight from '@astrojs/starlight';

// https://astro.build/config
export default defineConfig({
	integrations: [
		starlight({
			title: 'Serverless Storage',
			description: 'Pluggable object-store engine for OpenSearch: writer/reader split, scale-to-zero, dynamic resharding.',
			social: [
				{ icon: 'github', label: 'GitHub', href: 'https://github.com/huyz0/OpenSearch/tree/feature/pluggable-engine-per-shard-role/plugins/serverless-storage' },
			],
			customCss: ['./src/styles/mermaid-diagrams.css', './src/styles/layout.css'],
			components: {
				Head: './src/components/Head.astro',
			},
			sidebar: [
				{ label: 'Overview', slug: 'overview' },
				{
					label: 'Design',
					items: [
						{ label: 'Architecture', slug: 'design/architecture' },
						{ label: 'Coordination & Consistency', slug: 'design/coordination' },
						{ label: 'Remote Store Data Model', slug: 'design/remote-store-data-model' },
						{ label: 'Writer Engine & WAL', slug: 'design/writer-engine' },
						{ label: 'Reader Engine & Materialization', slug: 'design/reader-engine' },
						{ label: 'GC, Retention & PITR', slug: 'design/gc-retention' },
						{ label: 'Resharding', slug: 'design/resharding' },
						{ label: 'Scale-to-Zero & Scale-Up', slug: 'design/scale-to-zero-scale-up' },
						{ label: 'Allocation & Placement', slug: 'design/allocation' },
						{ label: 'Security & Compaction', slug: 'design/security-compaction' },
						{ label: 'REST API Surface', slug: 'design/rest-api' },
						{ label: 'Snapshot & Restore', slug: 'design/snapshot-restore-proposal' },
						{ label: 'Node Autoscaling', slug: 'design/node-autoscaling' },
					],
				},
				{
					label: 'Flows',
					items: [
						{ label: 'Core Sequences', slug: 'flows/core-sequences' },
						{ label: 'Resharding Sequences', slug: 'flows/resharding-sequences' },
						{ label: 'Scale-to-Zero Sequence', slug: 'flows/scale-to-zero-sequence' },
							{ label: 'Snapshot & Restore Sequence', slug: 'flows/snapshot-restore-sequence' },
					],
				},
				{ label: 'Interoperability', slug: 'interoperability' },
				{ label: 'Core Changes', slug: 'core-changes' },
				{ label: 'Configuration', slug: 'configuration' },
			],
		}),
	],
});
