package com.company;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;

import java.io.IOException;

public class Main {

    private static final String USER_AGENT = "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_11_3) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/55.0.2883.95 Safari/537.36";

    public static void main(String[] args) throws IOException {
	    String requestURL = "https://www.amazon.ca/s/ref=nb_sb_noss?field-keywords=nikon+SLR";
	    Document doc = Jsoup.connect(requestURL).userAgent(USER_AGENT).timeout(1000).get();

	    String elePath = "#result_3 > div > div > div > div.a-fixed-left-grid-col.a-col-right > div.a-row.a-spacing-small > div.a-row.a-spacing-none.scx-truncate-medium.sx-line-clamp-2 > a";
	    Element ele = doc.select(elePath).first();
	    if (ele != null) {
	        String detailUrl = ele.attr("href");
	        System.out.println("detail url = " + detailUrl);
			String title = ele.attr("title");
			System.out.println("title = " + title);
        } else {
            System.out.println("cant get ele");
		}

		String imagePath = "#result_3 > div > div > div > div.a-fixed-left-grid-col.a-col-left > div > div > a > img";
	    Element imgEle = doc.select(imagePath).first();
        if (imgEle != null) {
            String imgUrl = imgEle.attr("src");
            System.out.println("img url = " + imgUrl);
        } else {
            System.out.println("cant get img");
        }

        String pricePath = "#result_3 > div > div > div > div.a-fixed-left-grid-col.a-col-right > div:nth-child(2) > div.a-column.a-span7 > div.a-row.a-spacing-none > a > span.a-size-base.a-color-price.s-price.a-text-bold";
        Element priceEle = doc.select(pricePath).first();
        if (priceEle != null) {
            String priceStr = priceEle.text();
            System.out.println("priceStr = " + priceStr);
        } else {
            System.out.println("cant get price");
        }
    }
}
