def fetchaccinfo(username,timeout_limit=3):
    import requests
    from lxml import etree
    import re
    result = [None,None,None,None,None,{},{}]
    timeout_counter = 0
    url = 'https://twitter.com/'+username
    while True:
        try:
            r = requests.get(url,allow_redirects=False)
        except:
            if timeout_counter>=timeout_limit:
                result[0] = -1
                return result
            timeout_counter+=1
            continue
        break
    if r.status_code != 200:
        if r.status_code == 302:
            if 'location' in r.headers and r.headers['location']=='https://twitter.com/account/suspended':
                result[0] = -2
                return result
            else:
                result[0] = -3
                return result
        elif r.status_code == 404:
            result[0] = -4
            return result
        else:
            result[0] = -5
            print 'Unknown response status: ', r.status_code
            return result
    else:
        result[0] = 1
        tree = etree.HTML(r.text)
        elem = tree.xpath('body/div[1]/div[2]/div[1]/div[2]/div[2]/div[1]/ul[1]/li[3]/a[1]/strong')#followers
        result[1] = int(elem[0].text.replace(',',''))
        elem = tree.xpath('body/div[1]/div[2]/div[1]/div[2]/div[2]/div[1]/ul[1]/li[1]/a[1]/strong')#tweets
        result[2] = int(elem[0].text.replace(',',''))
        elem = tree.xpath('body/div[1]/div[2]/div[1]/div[2]/div[2]/div[1]/ul[1]/li[2]/a[1]/strong')#followings
        result[3] = int(elem[0].text.replace(',',''))
        elem = tree.xpath('body/div[1]/div[2]/div[1]/div[2]/div[1]/div[2]/p[1]/span[1]/span')#location
        if len(elem)==0:
            result[4] = 'Unavailable'
        else:
           if len(elem[0].text.strip())==0:
               result[4] = 'Unavailable'
           else:
               result[4] = elem[0].text.strip()
        atdict = {}
        m = re.findall('<s>@</s><b>.+?</b>',r.text)#at dictionary
        for item in m:
            t_name = item[11:-4]
            if t_name.lower()==username.lower():
                continue
            if t_name in atdict:
                atdict[t_name]+=1
            else:
                atdict[t_name]=1
        ponddict = {}
        m = re.findall('<s>#</s><b>.+?</b>',r.text)#pond dictionary
        for item in m:
            t_topic = item[11:-4]
            if t_topic in ponddict:
                ponddict[t_topic]+=1
            else:
                ponddict[t_topic]=1

        #fetching more data

        m = re.search('data-max-id=".+?"',r.text)
        if m==None:
            data_max_id = '-1'
        else:
            data_max_id = m.group(0)[13:-1]
        url = 'https://twitter.com/i/profiles/show/%s/timeline?include_available_features=1&include_entities=1&max_id=%s'%(username,data_max_id)

        while data_max_id!='-1':
            timeout_counter = 0
            flag_success = False
            while True:
                try:
                    r = requests.get(url)
                except:
                    if timeout_counter>=timeout_limit:
                        break
                    timeout_counter += 1
                    continue
                flag_success = True
                break
            if not flag_success:
                break
            if not r.status_code == 200:
                break
            m = re.search('"max_id":".+?"',r.text)
            if m==None:
                data_max_id = '-1'
            else:
                data_max_id = m.group(0)[10:-1]
            url = 'https://twitter.com/i/profiles/show/%s/timeline?include_available_features=1&include_entities=1&max_id=%s'%(username,data_max_id)

            m = re.findall('\\\\u003e\\\\u003cs\\\\u003e@\\\\u003c\\\\/s\\\\u003e\\\\u003cb\\\\u003e.+?\\\\u003c\\\\/b\\\\u003e',r.text)#at dictionary
            for item in m:
                t_name = item[48:-15]
                if t_name.lower()==username.lower():
                    continue
                if t_name in atdict:
                    atdict[t_name]+=1
                else:
                    atdict[t_name]=1
            m = re.findall('\\\\u003e\\\\u003cs\\\\u003e#\\\\u003c\\\\/s\\\\u003e\\\\u003cb\\\\u003e.+?\\\\u003c\\\\/b\\\\u003e',r.text)#pond dictionary
            for item in m:
                t_topic = item[48:-15]
                if t_topic in ponddict:
                    ponddict[t_topic]+=1
                else:
                    ponddict[t_topic]=1

        result[5] = atdict
        result[6] = ponddict
   
        return result

