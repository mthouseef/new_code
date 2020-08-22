news_data = [{
        "domain":"https://24.ae",
        "url":"https://24.ae/LatestNews.aspx",
        "news_url_xpath":"//div[contains(@id,'DivDataLists')]/article//@href",
        "title_xpath":"//h1[contains(@class,'post-title')]/text()",
        "description_xpath":"//div[contains(@class,'description')]/text()",
        "image_xpath":"//div[contains(@class,'post-featured')]/img/@src",
        "country": "ae/ar",
        "language": "arabic",
        "time_zone": "Asia/Dubai"
       },
       {
        "domain":"https://www.albayan.ae",
        "url":"https://www.albayan.ae/breaking-news",
        "news_url_xpath":"//li[contains(@class,'el')]/article//@href",
        "title_xpath":"//h1[contains(@class,'title')]/text()",
        "description_xpath":"//div[@id='articledetails']/p[contains(@dir,'RTL')]/text()",
        "image_xpath":"//figure[contains(@class,'detailsmedia')]//@src",
        "country": "ae/ar",
        "language": "arabic",
        "time_zone": "Asia/Dubai"
       },
       {
        "domain":"https://www.masrawy.com",
        "url":"https://www.masrawy.com",
        "news_url_xpath":"//a[contains(@class,'item')]/@href",
        "title_xpath":"//div[contains(@class,'articleHeader')]/h1/text()",
        "description_xpath":"//div[contains(@class,'ArticleDetails details')]/p/text()",
        "image_xpath":"//img[contains(@id,'FeedImage')]/@src",
        "country": "eg/ar",
        "language": "arabic",
        "time_zone": "Egypt"
       },
       {
        "domain":"https://www.alittihad.ae",
        "url":"https://www.alittihad.ae",
        "news_url_xpath":"//h5[contains(@class,'post-title')]//@href",
        "title_xpath":"//h1[contains(@class,'post-details-title')]/text()",
        "description_xpath":"//div[contains(@class,'post-content')]/p/text()",
        "image_xpath":"//figure[contains(@class,'post-media')]//@src",
        "country": "ae/ar",
        "language": "arabic",
        "time_zone": "Asia/Dubai"
       },
       {
        "domain":"https://www.emaratalyoum.com",
        "url":"https://www.emaratalyoum.com/",
        "news_url_xpath":"//ul[contains(@id,'mainslider')]/li//@href",
        "title_xpath":"//h1[contains(@class,'title')]/text()",
        "description_xpath":"//div[contains(@id,'articledetails')]/p/text()",
        "image_xpath":"//figure[contains(@class,'detailsmedia')]//@src",
        "country": "ae/ar",
        "language": "arabic",
        "time_zone": "Asia/Dubai"
       },
       {
        "domain":"https://www.emirates247.com",
        "url":"https://www.emirates247.com",
        "news_url_xpath":"//div[contains(@class,'pull-right')]//li//@href",
        "title_xpath":"//h1[contains(@class,'title')]/text()",
        "description_xpath":"//div[contains(@id,'articledetails')]/p/text()",
        "image_xpath":"//figure[contains(@class,'detailsmedia')]//@src",
        "country": "ae/en",
        "language": "english",
        "time_zone": "Asia/Dubai"
       },
       {
        "domain":"https://gulfnews.com",
        "url":"https://gulfnews.com",
        "news_url_xpath":"//h2[contains(@class,'card-title')]//@href",
        "title_xpath":"//h1[contains(@class,'')]/text()",
        "description_xpath":"//div[contains(@class,'story-block')]/p/text()",
        "image_xpath":"//figure[contains(@class,'article-main-image')]//@src",
        "country": "ae/en",
        "language": "english",
        "time_zone": "Asia/Dubai"
       },
       {
        "domain":"https://www.menaherald.com",
        "url":"https://www.menaherald.com/latest-news",
        "news_url_xpath":"//div[contains(@class,'cover')]//@href",
        "title_xpath":"//div[contains(@class,'pane-content')]/h1/text()",
        "description_xpath":"//div[contains(@class,'pane-node-field-body')]//p/text()",
        "image_xpath":"//img[contains(@class,'field-slideshow-image')]/@src",
        "country": "ae/ar",
        "language": "arabic",
        "time_zone": "Asia/Dubai"
       },
       {
        "domain":"https://www.ndtv.com",
        "url":"https://www.ndtv.com/latest?pfrom=home-mainnavgation",
        "news_url_xpath":"//div[contains(@class,'new_storylising_img')]//@href",
        "title_xpath":"//h1[contains(@class,'ttl')]/text()",
        "description_xpath":"//div[contains(@class,'expand-txt')]/p/text()",
        "image_xpath":"//div[contains(@class,'ins_instory_dv')]//@src",
        "country": "in/en",
        "language": "english",
        "time_zone": "Asia/Kolkata"
       },
       {
        "domain":"https://sabq.org",
        "url":"https://sabq.org/",
        "news_url_xpath":"//div[contains(@class,'news-block wow fadeIn')]/h4/a/@href",
        "title_xpath":"//div[contains(@itemprop,'headline')]/text()",
        "description_xpath":"//div[contains(@itemprop,'articleBody')]/p/text()",
        "image_xpath":"//div[contains(@class,'img-body')]//img/@src",
        "country": "sa/ar",
        "language": "arabic",
        "time_zone": "Asia/Riyadh"
       },
       {
        "domain":"https://www.sayidaty.net",
        "url":"https://www.sayidaty.net/",
        "news_url_xpath":"//div[contains(@class,'thumbnail')]/a/@href",
        "title_xpath":"//div[contains(@class,'entry-title')]/h1/text()",
        "description_xpath":"//div[contains(@class,'entry-content')]/p/text()",
        "image_xpath":"//div[contains(@class,'slick-slide-inner')]/img/@data-lazy",
        "country": "sa/ar",
        "language": "arabic",
        "time_zone": "Asia/Riyadh"
       },
       {
        "domain":"https://www.shorouknews.com",
        "url":"https://www.shorouknews.com/news/",
        "news_url_xpath":"//ul[contains(@class,'listing')]//div[contains(@class,'text')]//@href",
        "title_xpath":"//div[contains(@id,'sticky')]/h1/text()",
        "description_xpath":"//div[contains(@class,'eventContent')]/p/strong/text()",
        "image_xpath":"//img[contains(@id,'Body_Body_imageMain')]/@src",
        "country": "eg/ar",
        "language": "arabic",
        "time_zone": "Egypt"
       },
       {
        "domain":"https://www.thehindu.com/news/",
        "url":"https://www.thehindu.com/news/",
        "news_url_xpath":"//div[contains(@class,'justIn-story')]//h3//@href",
        "title_xpath":"//h1[contains(@class,'title')]/text()",
        "description_xpath":"//div[contains(@id,'content-body')]//p/text()",
        "image_xpath":"//img[contains(@class,'lead-img')]/@src",
        "country": "in/en",
        "language": "english",
        "time_zone": "Asia/Kolkata"
       },
       {
        "domain":"https://www.thehindubusinessline.com",
        "url":"https://www.thehindubusinessline.com/latest-news/",
        "news_url_xpath":"//ul[contains(@class,'latest-news')]/li//@href",
        "title_xpath":"//h1[contains(@class,'title')]/text()",
        "description_xpath":"//div[contains(@id,'content-body')]/p/text()",
        "image_xpath":"//img[contains(@class,'media-object')]/@src",
        "country": "in/en",
        "language": "english",
        "time_zone": "Asia/Kolkata"
       },
       {
        "domain":"https://www.thenational.ae",
        "url":"https://www.thenational.ae/international",
        "news_url_xpath":"//div[contains(@class,'small-article-desc')]/a/@href",
        "title_xpath":"//h1[contains(@class,'article-title')]/text()",
        "description_xpath":"//div[contains(@class,'readcls')]/p/text()",
        "image_xpath":"//figure//@src",
        "country": "ae/en",
        "language": "english",
        "time_zone": "Asia/Dubai"
       },
       {
        "domain":"https://www.yallakora.com",
        "url":"https://www.yallakora.com",
        "news_url_xpath":"//section[contains(@class,'pattern1')]//a[contains(@class,'link')]/@href",
        "title_xpath":"//h1[contains(@class,'artclHdline')]/text()",
        "description_xpath":"//div[contains(@class,'ArticleDetails details')]/p/text()",
        "image_xpath":"//img[contains(@id,'FeedImage')]/@data-src",
        "country": "eg/ar",
        "language": "arabic",
        "time_zone": "Egypt"
       }
       ]


       #ssh -i /home/dev/Downloads/quickakbhar.pem quickakhbar@ec2-3-21-233-247.us-east-2.compute.amazonaws.com
       #ssh -i /home/dev/Downloads/quickakbhar.pem ubuntu@ec2-3-21-233-247.us-east-2.compute.amazonaws.com