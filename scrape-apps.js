#!/usr/bin/env node

var request = require('sync-request');
var cheerio = require('cheerio');

var url = process.argv[2];

var fetchPage = null;
var page = 1;
if (process.argv.length > 3) {
  fetchPage = parseInt(process.argv[3]);
  page = fetchPage;
}


var apps = [];
while (true) {
  var body = request('GET', url + "&page=" + page).body;
  var $ = cheerio.load(body);
  var els = $('td:first-child a');
  var nextPageAnchor = $('h4 > span > a:last-child');
  els.each(function(id,el) { apps.push($(el).text()); });
  if (fetchPage != null || nextPageAnchor.html().trim() != "&gt;") break;
  page += 1;
}
console.log(apps.join('\n'));

