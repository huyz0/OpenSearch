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
			sidebar: [
				{ label: 'Overview', slug: 'overview' },
				{
					label: 'Design',
					items: [
						{ label: 'Architecture', slug: 'design/architecture' },
						{ label: 'Flows', slug: 'design/flows' },
					],
				},
				{ label: 'Core Changes', slug: 'changes' },
				{ label: 'Configuration', slug: 'configuration' },
			],
		}),
	],
});
