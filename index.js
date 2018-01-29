var mix = require('mixwith-es5').mix;
var EventEmitterMixin = require('event-emitter-mixin');
var uniq = require('array-uniq');
var compact = require('array-compact');
var defaults = require('defaults-es6');

class FeedEntry {
    constructor(opts) {
        this.indexFunc = opts.indexFunc;
        this.originalEntry = opts.originalEntry;
        this.dataSourceName = opts.dataSourceName;
        this.params = opts.params;
        this.tags = opts.tags;
    }
}

class FeedFetcher {
    constructor(opts) {
        this.dataSourceName = opts.dataSourceName;
        this.fetchFunc = opts.fetchFunc;
        this.indexFunc = opts.indexFunc;
        this.tags = opts.tags;
        this.requestTransform = opts.requestTransform;
        this.entryTransform = opts.entryTransform;
        this.entryTagsTransform = opts.entryTagsTransform;
        this.params = opts.params || {};
        this.isDepleted = false;
        this.loadedEntries = [];
        this.currentPromise = Promise.resolve();
    }

    fetch(target, minimumEntries) {
        if (isNaN(target)) throw "Invalid fetch cursor supplied to feed fetcher";
        if (!minimumEntries) minimumEntries = 0;

        target = Math.max(target, this.loadedEntries.length + minimumEntries);

        var cursor = this.loadedEntries.length;
        var numToRequest = target - this.loadedEntries.length;
        if (this.isDepleted || numToRequest <= 0) return Promise.resolve({entries: this.loadedEntries, isDepleted: this.isDepleted});

        return this.currentPromise
            .then(lastEntries => {
                var currentPage = Math.floor(cursor / numToRequest);
                var targetPage = currentPage + 1;
                var estimatedResults = targetPage * numToRequest;
                var currentResults = currentPage * numToRequest;
                var waste = currentResults - cursor;
                var extra = estimatedResults - target;
                if (FeedFetcher.VERBOSE_LOGGING) {
                    console.log("Shaping query for", this.params.toString(), "endpoint");
                    console.log("we currently have", cursor, "entries");
                    console.log("we want to have", target, "entries");
                    console.log("we will have", estimatedResults, "entries");
                    console.log("we will have downloaded the same", waste, "entries over again");
                    console.log("we will get", extra, "extra entries");
                }

                return this.currentPromise = Promise.resolve(this.fetchFunc(Object.assign({
                        limit: numToRequest,
                        page: targetPage,
                        offset: cursor
                    }, this.params)))
                    .then(loadedEntries => {
                        if (loadedEntries) {
                            return Promise.resolve(this.requestTransform ? this.requestTransform.call(this, loadedEntries) : loadedEntries)
                                .then(loadedEntries => {
                                    if (Array.isArray(loadedEntries)) {
                                        if (loadedEntries.length > numToRequest) {
                                            console.warn("We got", loadedEntries.length - numToRequest, "more results than we were expecting for", this.dataSourceName, "endpoint. Make sure the URL template is configured correctly.");
                                        }

                                        if (loadedEntries.length < numToRequest) {
                                            this.isDepleted = true;
                                        }

                                        return Promise.all(loadedEntries.map((entry, ii) => {
                                            return Promise.resolve(this.entryTransform ? this.entryTransform.call(this, entry) : entry)
                                                .then(entry => {
                                                    return new FeedEntry({
                                                        dataSourceName: this.dataSourceName,
                                                        params: this.params,
                                                        tags: this.entryTagsTransform ? this.entryTagsTransform.call(this, this.tags.slice(0), entry): this.tags.slice(0),
                                                        originalEntry: entry,
                                                        indexFunc: this.indexFunc
                                                    });
                                                });
                                        }))
                                        .then(newEntries => {
                                            return {isDepleted: this.isDepleted, entries: this.loadedEntries = this.loadedEntries.concat(newEntries)};
                                        });
                                    } else {
                                        this.isDepleted = true;
                                        console.warn('We got a response from fetcher, but not an array. Params:', this.params);
                                    }
                                });
                        } else {
                            //In this case, we assume the fetcher doesn't want to support the particular query set and we ignore it
                        }
                    }, err => {
                        this.isDepleted = true;
                        console.warn("request to endpoint failed: " + this.key);
                    });
            });
    }
}

function hashFromVal(val) {
    if (typeof val === 'string' || typeof val === 'number') {
        return val;
    } else if (val && typeof val === 'object') {
        if (Array.isArray(val)) {
            return compact(uniq(val.map(hashFromVal))).sort().join('.')
        } else {
            return Object.entries(val)
                .sort((a,b) => a[0] < b[0] ? -1 : 1)
                .map(entry => entry[0] + '_' + hashFromVal(entry[1])).join('.');
        }
    } else {
        return val;
    }
}

class FeedSource {
    constructor(opts) {
        this.name = opts.name;
        this.fetchFunc = opts.fetchFunc;
        this.indexFunc = opts.indexFunc;
        this.tags = opts.tags || [];
        this.fetchers = {};
        this.entryTransform = opts.entryTransform;
        this.entryTagsTransform = opts.entryTagsTransform;
        this.requestTransform = opts.requestTransform;
    }

    getFetcher(key, params) {
        if (!(key in this.fetchers)) {
            this.fetchers[key] = new FeedFetcher({
                params,
                entryTagsTransform: this.entryTagsTransform,
                tags: this.tags,
                dataSourceName: this.name,
                indexFunc: this.indexFunc,
                fetchFunc: this.fetchFunc,
                entryTransform: this.entryTransform,
                requestTransform: this.requestTransform
            })
        }
        return this.fetchers[key];
    }
}

module.exports = class extends mix(class FeedAggregator {}).with(EventEmitterMixin) {
    constructor(opts) {
        super(opts);
        this.dataSources = [];
        this.loadBuffer = opts.loadBuffer || 8;
        this.defaultParams = opts.params || {};
        this.entryTagsTransform = opts.entryTagsTransform;
        this.currentSet = [];
        this.sets = {};

        if (opts.dataSources) this.addDataSources(opts.dataSources);
    }

    addDataSources(dataSources) {
        this.dataSources = this.dataSources.concat(Object.entries(dataSources).map(entry => new FeedSource(Object.assign({entryTagsTransform: this.entryTagsTransform, name: entry[0]}, entry[1]))));
    }

    filter(params) {
        return this.fetch(0, params);
    }

    fetch(numToLoad, params, filterOr) {
        if (numToLoad && typeof numToLoad === 'object') {
            params = numToLoad;
            numToLoad = this.loadBuffer;
        } else if (isNaN(numToLoad)) {
            numToLoad = this.loadBuffer;
        }
        params = defaults(params, this.defaultParams);

        var key = hashFromVal(params);
        var fetchers = this.dataSources.map(dataSource => dataSource.getFetcher(key, params));
        var currSet = this.sets[key] || [];

        var currEntries = fetchers
            .reduce((allResults, currResult) => allResults.concat(currResult.loadedEntries), [])
            .sort((a, b) => a.index - b.index);

        var unusedEntries = currEntries.slice(currSet.length);
        var target = Math.max(currSet.length + numToLoad, this.loadBuffer);

        return Promise.all(fetchers
            .map(fetcher => {
                var fetcherUnused = unusedEntries.filter(entry => entry.dataSourceName === fetcher.dataSourceName);
                var fetcherTarget = fetcher.loadedEntries.length + numToLoad - fetcherUnused.length;
                return fetcher.fetch(fetcherTarget, this.loadBuffer)
            }))
            .then(fetcherResults => {
                var activeSources = this.dataSources.filter((val, key) => fetcherResults[key]);

                fetcherResults = compact(fetcherResults);

                var newSet = fetcherResults
                    .reduce((allResults, currResult) => allResults.concat(currResult.entries), []);

                var originalEntries = newSet.map(item => item.originalEntry);
                
                newSet = newSet
                    .map((entry,ii) => {
                        return {
                            index: entry.indexFunc.call(this, entry.originalEntry, ii, originalEntries),
                            value: entry
                        };
                    })
                    .sort((a,b) => a.index - b.index)
                    .map(obj => obj.value)

                var isDepleted = fetcherResults.map(result => result.isDepleted) && newSet.length <= target;

                newSet = newSet.slice(0, target);

                var evtObj = {oldSet: this.currentSet, newSet, isDepleted, activeSources };
                this.trigger('updateset', Object.assign({}, evtObj));

                this.currentSet = this.sets[key] = newSet;

                return Object.assign({}, evtObj);
            });
    }
}