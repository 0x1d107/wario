#!/bin/env python
import asyncio, argparse, sys, re
from os import makedirs
import urllib.parse as uparse
import urllib.robotparser
import os.path as opath
import aiohttp
from lxml import etree
import lxml.html as html
from tqdm.asyncio import tqdm


async def fetch(session, url):
    async with session.get(url) as response:
        if response.ok:
            return await response.text()


download_sem = asyncio.Semaphore(128)
use_query = False
rename = lambda i,x:x


async def download(session, url, mkd=True,idx=0):
    try:
        async with download_sem:
            async with session.get(url) as response:
                if not response.ok:
                    print(
                        f"Got http error while getting '{url}': {response.status}",
                        file=sys.stderr,
                    )
                    if response.status == 429 and response.headers.get("retry-after"):

                        print(
                            f"Rate limit handling: sleeping for {response.headers.get('retry-after')} seconds",
                            file=sys.stderr,
                        )
                        await asyncio.sleep(int(response.headers.get("retry-after")))
                        return await download(session, url, mkd)

                    return None, None
                content = response.content_type
                url = uparse.urlparse(url)
                path = uparse.unquote(url.path)
                params = uparse.unquote(url.query)
                name = opath.basename(path)
                location = url.netloc
                dirname = "./" + location + "/" + opath.dirname(path) if mkd else "."
                makedirs(dirname, exist_ok=True)
                if not len(name):
                    name = "index"
                if content == "text/html" and not (
                    name.endswith(".html") or name.endswith(".htm")
                ):
                    name += ".html"
                if use_query:
                    name += "?" + params
                chunksize = 2**8
                name=rename(idx,name)
                filename = dirname + "/" + name
                with open(filename, "wb") as f:
                    with tqdm(
                        response.content.iter_chunked(chunksize),
                        total=response.content_length // chunksize
                        if response.content_length
                        else None,
                        unit_scale=chunksize,
                        unit="byte",
                    ) as pbar:
                        pbar.set_description(f"{url.netloc}: {name}", True)
                        async for chunk in (pbar):
                            f.write(chunk)
                return content, filename
    except aiohttp.ClientError as e:
        print(f"Failed to get '{url}': {str(e)}", file=sys.stderr)
    except FileNotFoundError:
        print("Can't open file", file=sys.stderr)
    return None, None


crawled = set()


def convert_link(from_url, to_url, regex=""):

    f = uparse.urlparse(from_url)
    t = uparse.urlparse(to_url)
    #    print(f.path,t.path,regex)
    p = t.path + ('?'+uparse.unquote(t.query) if use_query else '')
    if f.netloc != t.netloc or not re.match(regex, p):
        return None
    topath = t.path
    if opath.basename(t.path) == "":
        topath += "index"

    if not "." in opath.basename(t.path) or topath.endswith(".php"):
        topath += ".html"
    if use_query:
        topath += "%3F" + uparse.unquote(t.query)
    frompath = opath.dirname(f.path)
    frompath = "/" + frompath
    new_url = opath.relpath(topath, frompath)
    # print(f"[{frompath}] {to_url} -> {new_url}")
    return new_url


async def extract_links(extractor, url, path):
    ps = await asyncio.subprocess.create_subprocess_exec(
        "sh", "-c", extractor, url, path, stdout=asyncio.subprocess.PIPE
    )
    stdout, _ = await ps.communicate()
    links = {line.strip() for line in stdout.decode("utf-8").splitlines()}
    return links


async def crawl(session, url, fn=None, regex="", custom=None, make_dirs=True,idx=0):
    doctype, path = await download(session, url, make_dirs,idx=idx)
    if path is None:
        return set()
    crawled.add(url)
    if custom:
        return await extract_links(custom, url, path)
    if doctype != "text/html":
        return set()
    tree = html.parse(path, base_url=url)
    doc = tree.getroot()
    if doc is None:
        return set()
    linkset = set()

    def fix_link(x):
        if fn is None:
            return x
        y = fn(url, x, regex)
        if y is None:
            # print('default',x)
            return x
        linkset.add(x)
        return y

    doc.make_links_absolute(url)
    doc.rewrite_links(fix_link)
    with open(path, "wb") as f:
        # print(path)
        f.write(html.tostring(doc, pretty_print=True))

    return linkset
def renamer(args,i,x):
    try:
        return str(args.rename).format(name=x,index=i)
    except KeyError:
        return x

async def main(args):
    global download_sem
    if args.concurrency:
        download_sem = asyncio.Semaphore(args.concurrency)
    if args.query:
        global use_query
        use_query = True
    if args.rename:
        global rename
        rename = lambda i,x:renamer(args,i,x) 
    if args.file:
        if args.file == '-':
            f=sys.stdin
            args.url += [
                line.split()[0] for line in f.read().strip().splitlines() if line
            ]
        else:
            with  open(args.file) as f:
                args.url += [
                    line.split()[0] for line in f.read().strip().splitlines() if line
                ]
    async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(None)) as session:
        if args.opml or args.rss:
            feed_urls = args.rss
            if args.opml:
                opml = etree.parse(args.opml)
                feed_urls += opml.xpath("//outline/@xmlUrl")
            links = []
            for feed in filter(
                None, await asyncio.gather(*[fetch(session, u) for u in feed_urls])
            ):
                try:
                    links += [
                        u.strip()
                        for u in etree.fromstring(feed.encode("utf-8")).xpath(
                            "//item/link/text()|//item/enclosure/@url"
                        )
                    ]
                except Exception as e:
                    print(e)
                    pass
            args.url += links
        if args.m3u:
            for line in (await fetch(session, args.m3u)).split("\n"):
                if not line.startswith("#"):
                    if "http" in args.m3u:
                        args.url.append(uparse.urljoin(args.m3u, line.strip()))
                    else:
                        args.url.append(line.strip())
        # print(args.url)
        it = 0
        if args.crawl:
            urlset = set(args.url)
            """
            coros = {asyncio.create_task(crawl(session,url,fn = convert_link)) for url in urlset}
            while coros:
                done,coros = await asyncio.wait(coros,return_when=asyncio.FIRST_COMPLETED)
                for d in done:
                    newset = (await d).difference(crawled)
                    for url in newset:
                        if url not in crawled:
                            coros.add(asyncio.create_task(crawl(session,url,fn = convert_link)))
            """
            while len(urlset) > 0 and (
                args.depth is None or args.depth < 0 or it < args.depth
            ):

                nextsets = await asyncio.gather(
                    *[
                        crawl(
                            session,
                            url,
                            fn=convert_link,
                            regex=args.regex if args.regex else "",
                            custom=args.exec,
                            make_dirs=args.make_dirs,
                            idx=i
                        )
                        for i,url in enumerate(urlset)
                    ]
                )
                if len(nextsets) > 0:
                    urlset = nextsets[0].union(*nextsets[1:]).difference(crawled)
        # '''
        else:
            await asyncio.gather(
                *[download(session, url, args.make_dirs,idx=i) for i,url in
                    enumerate(args.url)]
            )


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Asynchronous http file downloader")
    parser.add_argument(
        "-d",
        "--make-dirs",
        action="store_true",
        help="Create directories to match url path",
    )
    parser.add_argument("--opml", help="Use urls from rss feeds in opml file")
    parser.add_argument(
        "--rss", action="append", default=[], help="Use urls from rss feed"
    )
    parser.add_argument("--m3u", help="Download m3u playlist")
    parser.add_argument("url", nargs="*", default=[], help="Url to file")
    parser.add_argument(
        "-c", "--concurrency", type=int, help="Number of concurrent downloads"
    )
    parser.add_argument(
        "-r", "--crawl", action="store_true", help="Recursively crawl website"
    )
    parser.add_argument("--regex", help="Restrict crawler to regex")
    parser.add_argument(
        "-f","--file", help="Read URLs from file. Each URL should be on a separate line."
    )
    parser.add_argument('--rename',help="Rename downloaded files according to pattern")
    parser.add_argument(
        "--exec",
        help="Extract links using shell command. Use it in conjunction with --crawl",
    )
    parser.add_argument(
        "-q", "--query", action="store_true", help="Save query parameters in file name"
    )
    parser.add_argument("-n", "--depth", help="Depth of crawling", type=int)
    asyncio.run(main(parser.parse_args()))
