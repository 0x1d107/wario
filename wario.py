#!/bin/env python
import asyncio, argparse, sys
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


async def download(session, url, mkd=True):
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
                name = opath.basename(path)
                location = url.netloc
                dirname = "./" + location + "/" + opath.dirname(path) if mkd else "."
                makedirs(dirname, exist_ok=True)
                if not len(name):
                    name = "index.html"
                if content == "text/html" and not (
                    name.endswith(".html") or name.endswith(".htm")
                ):
                    name += ".html"
                chunksize = 2**8
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
    return None, None


crawled = set()


def convert_link(from_url, to_url, subtree=""):

    f = uparse.urlparse(from_url)
    t = uparse.urlparse(to_url)
    # print(f.path,t.path,subtree)
    if f.netloc != t.netloc or not t.path.startswith(subtree):
        return None
    topath = t.path
    if opath.basename(t.path) == "":
        topath += "index"
    if not "." in opath.basename(t.path):
        topath += ".html"
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


async def crawl(session, url, fn=None, subtree="", custom=None,make_dirs=True):
    doctype, path = await download(session, url,make_dirs)
    crawled.add(url)
    if custom:
        return await extract_links(custom, url, path)
    if path is None or doctype != "text/html":
        return set()
    tree = html.parse(path, base_url=url)
    doc = tree.getroot()
    if doc is None:
        return set()
    linkset = set()

    def fix_link(x):
        if fn is None:
            return x
        y = fn(url, x, subtree)
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


async def main(args):
    global download_sem
    if args.concurrency:
        download_sem = asyncio.Semaphore(args.concurrency)
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
        # print(args.url)
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
            while len(urlset) > 0:
                nextsets = await asyncio.gather(
                    *[
                        crawl(
                            session,
                            url,
                            fn=convert_link,
                            subtree=args.subtree if args.subtree else "",
                            custom=args.exec,
                            make_dirs=args.make_dirs,
                        )
                        for url in urlset
                    ]
                )
                if len(nextsets) > 0:
                    urlset = nextsets[0].union(*nextsets[1:]).difference(crawled)
        # '''
        else:
            await asyncio.gather(
                *[download(session, url, args.make_dirs) for url in set(args.url)]
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
    parser.add_argument("url", nargs="*", default=[], help="Url to file")
    parser.add_argument(
        "-c", "--concurrency", type=int, help="Number of concurrent downloads"
    )
    parser.add_argument(
        "-r", "--crawl", action="store_true", help="Recursively crawl website"
    )
    parser.add_argument("--subtree", help="Restrict crawler to subtree")
    parser.add_argument(
        "--exec",
        help="Extract links using shell command. Use it in conjunction with --crawl",
    )
    asyncio.run(main(parser.parse_args()))
