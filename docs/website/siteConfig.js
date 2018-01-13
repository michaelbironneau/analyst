/**
 * Copyright (c) 2017-present, Facebook, Inc.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

/* List of projects/orgs using your project for the users page */
const users = [
  {
    caption: 'User1',
    image: '/analyst/img/docusaurus.svg',
    infoLink: 'https://github.com/michaelbironneau/analyst',
    pinned: true,
  },
];

const siteConfig = {
  title: 'Analyst QL' /* title for your website */,
  tagline: 'a declarative language for ETL jobs',
  url: 'https://github.com/michaelbironneau/analyst' /* your website url */,
  baseUrl: '/analyst/' /* base url for your project */,
  projectName: 'analyst',
  headerLinks: [
    {doc: 'intro', label: 'Introduction to Analyst'},
    {page: 'help', label: 'Reference'},
    {blog: true, label: 'Blog'},
  ],
  users,
  /* path to images for header/footer */
  headerIcon: 'img/docusaurus.svg',
  footerIcon: 'img/docusaurus.svg',
  favicon: 'img/favicon.png',
  /* colors for website */
  colors: {
    primaryColor: '#486A87',
    secondaryColor: '#FF9D00',
  },
  // This copyright info is used in /core/Footer.js and blog rss/atom feeds.
  copyright:
    'Copyright © ' +
    new Date().getFullYear() +
    ' Michael Bironneau',
  // organizationName: 'deltice', // or set an env variable ORGANIZATION_NAME
  // projectName: 'analyst', // or set an env variable PROJECT_NAME
  highlight: {
    // Highlight.js theme to use for syntax highlighting in code blocks
    theme: 'default',
  },
  scripts: ['https://buttons.github.io/buttons.js'],
  // You may provide arbitrary config keys to be used as needed by your template.
  repoUrl: 'https://github.com/michaelbironneau/analyst',
};

module.exports = siteConfig;
