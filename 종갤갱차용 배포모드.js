// ==UserScript==
// @name         숲종갤용 자동 갱차
// @namespace    http://tampermonkey.net/
// @version      1.0
// @description  갱차 파싱 및 자동 갱차 by 결챠니
// @match        https://gall.dcinside.com/*
// @grant        GM_xmlhttpRequest
// @grant        GM_setValue
// @grant        GM_getValue
// @require      https://code.jquery.com/jquery-3.6.0.min.js
// @connect      docs.google.com
// @run-at       document-end
// ==/UserScript==

(async function() {
    'use strict';

    const CONFIG = {
        SHEET_ID: '1gGoeiHZ21ob86Xri7FJf52avmlvgFdG-o-7hSDlCmZ0',
        BAN_DURATION: 744, // 31
        REALTIME_CONTENT_LOAD: true,
        MAX_CONCURRENT_LOADS: 5
    };

    let googleSheetBanList = new Set();
    let googleSheetMonitoring = false;
    let bannedPost = new Set();
    let PP_ON = false;
    let monitoringInterval = null;
    let ubw_processing = false;

    let currentDisplayedPosts = new Map();
    let currentDisplayedComments = new Map();
    let processedContentIds = new Set();
    let contentLoadingInProgress = false;
    let backgroundLoadingActive = false;

    // Active load variable
    let realtimeLoadQueue = new Set();
    let activeLoadPromises = new Map();
    let realtimeLoadingActive = false;

    let originalTableHTML = '';
    let isTableModified = false;
    let firstMonitoringRun = true;


    let DCMOD_MEMO = {};
    let MGALL_PERMABAN_KEY = [];
    let MGALL_PERMABAN_INFO = {};

    const GLOBAL_GALLERY_TYPESTR = getGalleryType();
    const _GALLERY_TYPE_ = (GLOBAL_GALLERY_TYPESTR === 'mgallery') ? 'M' :
                          (GLOBAL_GALLERY_TYPESTR === 'mini') ? 'MI' : 'G';
    const BAN_VALID_TIMES = [1,6,24,168,336,744];

    // basic of helper codes
    const $$ = window.jQuery || window.$ || $;
    const $ = {
        getURLParam: function(name) {
            const urlParams = new URLSearchParams(window.location.search);
            return urlParams.get(name);
        }
    };

    function sleep(ms) {
        return new Promise((r) => setTimeout(r, ms));
    }

    function gmRequestPromise(details) {
        return new Promise((resolve, reject) => {
            GM_xmlhttpRequest({
                ...details,
                onload: function (response) {
                    resolve(response);
                },
                onerror: function (error) {
                    reject(error);
                },
                ontimeout: function () {
                    reject(new Error('Request timed out'));
                }
            });
        });
    }

    function get_cookie(name) {
        const value = `; ${document.cookie}`;
        const parts = value.split(`; ${name}=`);
        if (parts.length === 2) return parts.pop().split(';').shift();
        return '';
    }

    function get_gall_type_name() {
        const path = window.location.pathname;
        if (path.includes('mgallery')) return 'mgallery';
        if (path.includes('mini')) return 'mini';
        return '';
    }

    function getGalleryType() {
        const path = window.location.pathname;
        if (path.includes('mgallery')) return 'mgallery';
        if (path.includes('mini')) return 'mini';
        return '';
    }

    function getGallid() {
        return $.getURLParam('id');
    }

    // backup the original table
    function backupOriginalTable() {
        const gallList = document.querySelector('table.gall_list');
        if (gallList && !isTableModified) {
            originalTableHTML = gallList.outerHTML;
        }
    }

    function restoreOriginalTable() {
        if (originalTableHTML) {
            const gallList = document.querySelector('table.gall_list');
            if (gallList) {
                const tempDiv = document.createElement('div');
                tempDiv.innerHTML = originalTableHTML;
                const restoredTable = tempDiv.firstChild;

                gallList.parentNode.replaceChild(restoredTable, gallList);
                isTableModified = false;

                setTimeout(() => {
                    window.location.reload();
                }, 500);
            }
        }
    }

    function classifyPostType(post) {
        try {
            const postId = post.getAttribute('data-no');

            const hasNoticeIcon = post.querySelector('.icon_img.icon_notice');
            if (hasNoticeIcon) {
                return { type: 'notice', shouldDisplay: false, shouldCheckForBan: false };
            }

            const datatype = post.getAttribute('data-type');
            if (datatype === 'icon_notice') {
                return { type: 'notice', shouldDisplay: false, shouldCheckForBan: false };
            }

            const isGallManager_s = post.querySelector('div.gallview_head div.gall_writer a.writer_nikcon img')?.src;
            const isGallManager = isGallManager_s && (
                isGallManager_s.includes('managernik.gif') ||
                isGallManager_s.includes('sub_managernik.gif') ||
                isGallManager_s.includes('fix_sub_managernik.gif') ||
                isGallManager_s.includes('fix_managernik.gif')
            );

            if (isGallManager) {
                return { type: 'admin_notice', shouldDisplay: false, shouldCheckForBan: false };
            }

            const hasSlowIcon = post.querySelector('.icon_img.icon_slow');
            if (hasSlowIcon) {
                return { type: 'pinned', shouldDisplay: true, shouldCheckForBan: true };
            }

            return { type: 'normal', shouldDisplay: true, shouldCheckForBan: true };

        } catch (error) {
            console.error('말머리 확인 오류:', error);
            return { type: 'normal', shouldDisplay: true, shouldCheckForBan: true };
        }
    }

    // Comment Statement
    function saveCommentState(comment, additionalInfo = {}) {
        const cid = comment.getAttribute('data-cmt');
        if (!cid) return;

        const clonedComment = comment.cloneNode(true);
        const commentState = {
            element: clonedComment,
            lastUpdated: Date.now(),
            ...additionalInfo
        };

        currentDisplayedComments.set(cid, commentState);
    }

    function restoreCommentFromState(cid) {
        const commentState = currentDisplayedComments.get(cid);
        if (!commentState) return null;

        const restoredComment = commentState.element.cloneNode(true);
        return restoredComment;
    }

    function savePostState(post, additionalInfo = {}) {
        const pid = post.getAttribute('data-no');
        if (!pid) return;

        const clonedPost = post.cloneNode(true);
        const postState = {
            element: clonedPost,
            hasContent: processedContentIds.has(pid),
            postType: classifyPostType(post),
            lastUpdated: Date.now(),
            ...additionalInfo
        };

        currentDisplayedPosts.set(pid, postState);
    }

    function restorePostFromState(pid) {
        const postState = currentDisplayedPosts.get(pid);
        if (!postState) return null;

        const restoredPost = postState.element.cloneNode(true);
        return restoredPost;
    }

    function calculateLength(str) {
        let length = 0;
        for (let i = 0; i < str.length; i++) {
            length += (str[i].match(/[ㄱ-힣]/)) ? 2 : 1;
        }
        return length;
    }

    function truncateString(str, allowedLen) {
        if (allowedLen < 3) {
            return str.substring(0,2)+'..';
        }
        if (str.length <= allowedLen) {
            return str;
        }
        return str.substring(0, Math.max(2, allowedLen - 3)) + '..';
    }

    function tooltipspliter(strArr) {
        let str = "";
        let len = 0;
        if (typeof(strArr) == 'object') {
            for (let cur of strArr) {
                if (len > 24) {
                    len = 0;
                    str += '<br />';
                }
                str += `${cur}, `;
                len += calculateLength(cur);
            }
            return str.slice(0, -2);
        } else if (typeof(strArr) == 'string') {
            let tmparr = strArr.split('');
            for (let cur of tmparr) {
                if (len > 32) {
                    len = 0;
                    str += '<br />';
                }
                str += cur;
                len += calculateLength(cur);
            }
            return str;
        }
    }

    async function getmemo(target_id) {
        let isHavePermabanHistory = false;
        let permabanGall = [];

        if (!target_id.includes('.')) {
            for (let cur of MGALL_PERMABAN_KEY) {
                for (let uid of MGALL_PERMABAN_INFO[cur]) {
                    if (uid == target_id) {
                        permabanGall.push(cur);
                        isHavePermabanHistory = true;
                    }
                }
            }
        }

        let hasMemoOrPban = false;
        if (DCMOD_MEMO[target_id] != undefined || isHavePermabanHistory) {
            hasMemoOrPban = true;
        }

        return [hasMemoOrPban, target_id, permabanGall, DCMOD_MEMO[target_id]];
    }

    async function process_ubwriter(nodel, noGmemo) {
        if (ubw_processing == true) return;
        ubw_processing = true;

        try {
            let writers = document.querySelectorAll('.ub-writer');
            let deletable = document.querySelectorAll('.DCMOD_DELETABLE');

            if (nodel != true) {
                for (let cur of deletable) {
                    cur.parentNode.removeChild(cur);
                }
            }

            for (let cur of writers) {
                if (nodel == true && cur.querySelector('.DCMOD_DELETABLE') != null) continue;

                let uid = cur.getAttribute('data-uid');
                let ip = cur.getAttribute('data-ip');
                let process_target = cur.querySelector('span.nickname em');

                if (process_target == null) continue;

                let allow_len = 16 - calculateLength(process_target.textContent.trim());

                if (uid != null && uid.length > 2) {
                    await processFixedUser(cur, uid, allow_len);
                } else if (ip != null && ip.length > 2) {
                    await processFloatingUser(cur, ip, allow_len);
                }
            }
        } catch(e) {
            console.error('작성자 정보 처리 오류:', e);
        } finally {
            ubw_processing = false;
        }
    }

    async function processFixedUser(cur, uid, allow_len) {
        let wid = document.createElement('span');
        let curMemo = await getmemo(uid);

        let memo_finalstr = '';
        if (curMemo[2].length != 0) {
            if (curMemo[3] != null) {
                memo_finalstr = '⚠️'+curMemo[3];
            } else {
                memo_finalstr = '⚠️'+curMemo[1];
            }
        } else {
            if (curMemo[3] == null) {
                memo_finalstr = curMemo[1];
            } else {
                memo_finalstr = curMemo[3];
            }
        }

        let memo_min = truncateString(memo_finalstr, allow_len);
        wid.textContent = memo_min;
        wid.setAttribute('class', 'DCMOD_DELETABLE');

        if (curMemo[3] != null) {
            wid.setAttribute('class', 'DCMOD_DELETABLE DCMOD_REDTEXT');
        }

        cur.appendChild(wid);

        let wid2 = document.createElement('span');
        wid2.innerHTML = tooltipspliter(uid);

        if (curMemo[3] != null) {
            wid2.innerHTML += `<br /><x style="color: rgb(255, 144, 144);">${tooltipspliter(curMemo[3])}</x>`;
        }

        if (curMemo[2].length != 0) {
            wid2.innerHTML += `<br /><x style="color: rgb(255, 144, 0);">⚠️[${curMemo[2]}] 갤러리에서 영구 차단됨⚠️<br />자세한 내용은 해당 갤러리의 갱차목록을 확인해 주세요.</x>`;
        }                                                                                                              // reffered by DC-modtools

        wid2.setAttribute('class', 'DCMOD_DELETABLE tooltip-text');
        cur.appendChild(wid2);
    }

    async function processFloatingUser(cur, ip, allow_len) {
        let curMemo = await getmemo(ip);

        if (curMemo[3] != null) {
            let wid = document.createElement('span');
            let memo_finalstr = truncateString(curMemo[3], allow_len);
            wid.textContent = memo_finalstr;
            wid.setAttribute('class', 'DCMOD_DELETABLE DCMOD_REDTEXT');
            cur.appendChild(wid);

            let wid2 = document.createElement('span');
            wid2.innerHTML = `<x style="color: rgb(255, 144, 144);">${tooltipspliter(curMemo[3])}</x>`;
            wid2.setAttribute('class', 'DCMOD_DELETABLE tooltip-text');
            cur.appendChild(wid2);
        }
    }

    // BanModule
    async function banModule_single(reason, postNo, replyNo, bantime, delete_it, ban_ip) {
        if (postNo == null || delete_it == null || ban_ip == null || bantime == null || !BAN_VALID_TIMES.includes(Number(bantime))) return false;
        let banA = null;
        let banB = null;
        if (replyNo != null) {
            banA = [replyNo];
            banB = postNo;
        } else {
            banA = [postNo];
            banB = null;
        }
        let ban_reason = '?';
        if (reason != null && reason.length > 0) {
            ban_reason = reason;
        }
        let ban_ip_chk = (ban_ip == null) ? 0 : ban_ip;
        return await $$.ajax({
            type : "POST",
            url : "/ajax/"+ get_gall_type_name() +"_manager_board_ajax/update_avoid_list",
            data : { ci_t : get_cookie('ci_c'),
                    id: $.getURLParam('id'),
                    nos : banA,
                    parent: banB,
                    avoid_hour : bantime,
                    avoid_reason : 0,
                    avoid_reason_txt : ban_reason,
                    del_chk : delete_it,
                    _GALLTYPE_: _GALLERY_TYPE_,
                    avoid_type_chk: ban_ip_chk},
            dataType : 'json',
            cache : false,
            async : false,
            success : function(ajaxData) {
                if(typeof(ajaxData.msg) != 'undefined' && ajaxData.msg) {
                    console.log(ajaxData.msg);
                } else {
                    console.log(ajaxData);
                }
            },
            error : function(ajaxData) {
                console.log('시스템 오류로 중지되었습니다. 잠시 후 다시 이용해 주세요.');
            }
        });
    }

    // fetchGoogleSheet
    async function fetchGoogleSheetData() {
        const btn = document.querySelector('#DCMOD_GOOGLESHEET_BTN');
        if (btn) {
            btn.style.backgroundColor = '#ffa500';
            btn.textContent = '갱신중';
        }

        try {
            const url = `https://docs.google.com/spreadsheets/d/${CONFIG.SHEET_ID}/export?format=csv&gid=0`;

            const response = await new Promise((resolve, reject) => {
                GM_xmlhttpRequest({
                    method: 'GET',
                    url: url,
                    timeout: 15000,
                    onload: resolve,
                    onerror: reject
                });
            });

            const csvText = response.responseText;
            const rows = csvText.split('\n');

            const uidList = rows
                .slice(1)
                .map(row => {
                    const cols = row.split(',');
                    return cols[0] ? cols[0].trim().replace(/^"|"$/g, '') : '';
                })
                .filter(uid => uid.length > 0);

            googleSheetBanList = new Set(uidList);

            await GM.setValue('googleSheetBanList', Array.from(googleSheetBanList));

            if (btn) {
                btn.style.backgroundColor = '#5cb85c';
                btn.textContent = `${googleSheetBanList.size}`;

                setTimeout(() => {
                    btn.style.backgroundColor = '#007bff';
                    btn.textContent = '목록갱신';
                }, 2000);
            }

        } catch (error) {
            console.error('데이터 로드 실패:', error);

            if (btn) {
                btn.style.backgroundColor = '#d9534f';
                btn.textContent = '갱신실패';

                setTimeout(() => {
                    btn.style.backgroundColor = '#007bff';
                    btn.textContent = '목록갱신';
                }, 3000);
            }
        }
    }

    async function fetchPostContent(postNo, retryCount = 0) {
        const maxRetries = 1;
        const baseDelay = 500;

        try {
            const response = await gmRequestPromise({
                method: 'GET',
                url: `https://gall.dcinside.com/${GLOBAL_GALLERY_TYPESTR}/board/view/?id=${$.getURLParam('id')}&no=${postNo}`,
                timeout: 6000,
                headers: {
                    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
                }
            });

            if (response.status !== 200) {
                throw new Error(`HTTP ${response.status}`);
            }

            const doc = new DOMParser().parseFromString(response.responseText, 'text/html');
            const contentDiv = doc.querySelector('div.write_div');

            if (contentDiv) {
                return contentDiv.innerHTML;
            } else {
                return null;
            }

        } catch (error) {
            if (retryCount < maxRetries) {
                const delayTime = baseDelay * (retryCount + 1);
                await sleep(delayTime);
                return fetchPostContent(postNo, retryCount + 1);
            }
            return null;
        }
    }

    async function addContentToPost(postElement, pid, content) {
        try {
            if (processedContentIds.has(pid)) {
                return true;
            }

            const currentPost = document.querySelector(`tr[data-no="${pid}"]`);
            if (!currentPost) {
                return false;
            }

            const titleCell = currentPost.querySelector('td.gall_tit, .gall_tit');
            if (!titleCell) {
                return false;
            }

            const existingPreview = titleCell.querySelector('.post-content-preview');
            if (existingPreview) {
                processedContentIds.add(pid);
                savePostState(currentPost, { contentAdded: true });
                return true;
            }

            const contentDiv = document.createElement('div');
            contentDiv.className = 'post-content-preview';
            contentDiv.innerHTML = content;

            contentDiv.style.cssText = `
                margin-top: 8px !important;
                padding: 10px !important;
                background-color: #f0f8ff !important;
                border: 1px solid #4CAF50 !important;
                border-left: 3px solid #4CAF50 !important;
                border-radius: 3px !important;
                max-height: 250px !important;
                overflow-y: auto !important;
                font-size: 12px !important;
                line-height: 1.4 !important;
                word-break: break-word !important;
                animation: realtimeContentLoad 0.5s ease;
                box-shadow: 0 2px 4px rgba(76, 175, 80, 0.2) !important;
            `;

            const images = contentDiv.querySelectorAll('img');
            images.forEach(img => {
                img.style.maxWidth = '100%';
                img.style.height = 'auto';
            });

            titleCell.appendChild(contentDiv);
            processedContentIds.add(pid);

            savePostState(currentPost, { contentAdded: true, realtimeLoaded: true });

            return true;

        } catch (error) {
            console.error(`게시글 ${pid} 본문 표시 오류:`, error);
            return false;
        }
    }

    async function loadNewPostsRealtime(newPostIds) {
        if (!CONFIG.REALTIME_CONTENT_LOAD || !googleSheetMonitoring || newPostIds.length === 0) {
            return;
        }

        const batches = [];
        for (let i = 0; i < newPostIds.length; i += CONFIG.MAX_CONCURRENT_LOADS) {
            batches.push(newPostIds.slice(i, i + CONFIG.MAX_CONCURRENT_LOADS));
        }

        for (const batch of batches) {
            if (!googleSheetMonitoring) break;

            const loadPromises = batch.map(async (pid) => {
                try {
                    if (activeLoadPromises.has(pid) || processedContentIds.has(pid)) {
                        return;
                    }

                    const loadPromise = fetchPostContent(pid);
                    activeLoadPromises.set(pid, loadPromise);

                    const content = await loadPromise;

                    if (content && googleSheetMonitoring) {
                        await addContentToPost(null, pid, content);
                    }

                } catch (error) {

                } finally {
                    activeLoadPromises.delete(pid);
                }
            });

            await Promise.allSettled(loadPromises);

            if (batches.indexOf(batch) < batches.length - 1) {
                await sleep(200);
            }
        }
    }

    function identifyNewPosts(allItems) {
        const newPostIds = [];

        for (const item of allItems) {
            if (item.type === 'post') {
                const pid = item.element.getAttribute('data-no');
                if (pid && !currentDisplayedPosts.has(pid) && !processedContentIds.has(pid)) {
                    newPostIds.push(pid);
                }
            }
        }

        return newPostIds;
    }

    async function googleSheetMonitor() {
        if (PP_ON || !googleSheetMonitoring) return;
        PP_ON = true;

        try {
            const btn = document.querySelector('#DCMOD_GOOGLESHEET_MONITOR_BTN');
            if (btn) {
                btn.setAttribute('style','background: #ff0000; color: #fff; border-color: #ff0000;');
            }

            const galleryId = $.getURLParam('id');

            const [postResp, replyResp] = await Promise.all([
                fetch(`https://gall.dcinside.com/${GLOBAL_GALLERY_TYPESTR}/board/lists/?id=${galleryId}`, { credentials: 'include' }),
                fetch(`https://gall.dcinside.com/${GLOBAL_GALLERY_TYPESTR}/board/lists/?id=${galleryId}&s_type=search_comment&s_keyword=.25`, { credentials: 'include' })
            ]);

            const postData = new DOMParser().parseFromString(await postResp.text(), "text/html");
            const replyData = new DOMParser().parseFromString(await replyResp.text(), "text/html");

            const posts = postData.querySelectorAll('table.gall_list > tbody > tr.us-post');
            const replies = replyData.querySelectorAll('table.gall_list > tbody > tr.search.search_comment');

            const allItems = [];
            const ban_arr = [];

            for (const post of posts) {
                const datatype = post.getAttribute('data-type');
                const pid = post.getAttribute('data-no');

                const postClassification = classifyPostType(post);

                if (!postClassification.shouldDisplay) {
                    continue;
                }

                allItems.push({ type: 'post', element: post });

                if (postClassification.shouldCheckForBan &&
                    (datatype == 'icon_pic' || datatype == 'icon_txt' || datatype == 'icon_movie')) {

                    const uid = post.querySelector('td.ub-writer')?.getAttribute('data-uid');
                    const title = post.querySelector('td.gall_tit a')?.textContent;

                    if (pid && uid && googleSheetBanList.has(uid) && !bannedPost.has(pid)) {
                        ban_arr.push([pid, title, uid, 'post']);
                        bannedPost.add(pid);
                    }
                }
            }


            for (const reply of replies) {
                const datacmt = reply.getAttribute('data-cmt');
                if (!datacmt) continue;

                allItems.push({ type: 'comment', element: reply });

                const uid = reply.querySelector('td.ub-writer')?.getAttribute('data-uid');
                const c_info = datacmt.split('_');

                if (uid && googleSheetBanList.has(uid) && !bannedPost.has(c_info[1])) {
                    ban_arr.push([c_info[1], c_info[0], uid, 'reply']);
                    bannedPost.add(c_info[1]);
                }
            }


            const newPostIds = identifyNewPosts(allItems);
            if (newPostIds.length > 0 && CONFIG.REALTIME_CONTENT_LOAD) {
                loadNewPostsRealtime(newPostIds);
            }

            await updateTableWithStatePreservation(allItems, ban_arr);

            if (ban_arr.length > 0) {
                for (const item of ban_arr) {
                    try {
                        let result;
                        if (item[3] === 'post') {
                            result = await banModule_single('갱신차단', item[0], null, CONFIG.BAN_DURATION, 1, 1);
                        } else if (item[3] === 'reply') {
                            result = await banModule_single('갱신차단', item[1], item[0], CONFIG.BAN_DURATION, 1, 1);
                        }

                        await sleep(300);

                    } catch (error) {
                        console.error(`차단 오류: ${item[2]}`, error);
                    }
                }
            }

            await process_ubwriter();
            firstMonitoringRun = false;

        } catch(e) {
            console.error('모니터링 오류:', e);
        } finally {
            PP_ON = false;
        }
    }

    async function updateTableWithStatePreservation(allItems, ban_arr) {
        const tbl = document.querySelector('table.gall_list tbody');
        if (!tbl) return;

        isTableModified = true;

        if (firstMonitoringRun || (currentDisplayedPosts.size === 0 && currentDisplayedComments.size === 0)) {
            await buildInitialTable(allItems, ban_arr, tbl);
            return;
        }

        const currentPostIds = new Set();
        const currentCommentIds = new Set();

        allItems.forEach(item => {
            if (item.type === 'post') {
                const pid = item.element.getAttribute('data-no');
                if (pid) currentPostIds.add(pid);
            } else if (item.type === 'comment') {
                const cid = item.element.getAttribute('data-cmt');
                if (cid) currentCommentIds.add(cid);
            }
        });

        tbl.innerHTML = '';

        const gallList = document.querySelector('table.gall_list');
        if (gallList.querySelector('colgroup')) {
            gallList.querySelector('colgroup').innerHTML = `
                <col style="width:7%">
                <col style="width:51px">
                <col>
                <col style="width:18%">
                <col style="width:6%">
                <col style="width:6%">
                <col style="width:6%">`;
        }

        const chkboxhead = gallList.querySelector('thead th.chkbox_th');
        if (chkboxhead) chkboxhead.parentNode.removeChild(chkboxhead);

        for (const item of allItems) {
            let finalElement;
            let itemId;

            if (item.type === 'post') {
                const pid = item.element.getAttribute('data-no');
                if (!pid) continue;
                itemId = pid;

                if (currentDisplayedPosts.has(pid)) {
                    finalElement = restorePostFromState(pid);
                } else {
                    finalElement = item.element.cloneNode(true);
                    savePostState(finalElement);
                }
            } else if (item.type === 'comment') {
                const cid = item.element.getAttribute('data-cmt');
                if (!cid) continue;
                itemId = cid;

                if (currentDisplayedComments.has(cid)) {
                    finalElement = restoreCommentFromState(cid);
                } else {
                    finalElement = item.element.cloneNode(true);
                    saveCommentState(finalElement);
                }
            }

            if (!finalElement) continue;

            for (const banItem of ban_arr) {
                const itemNo = finalElement.querySelector('td.gall_num')?.textContent.trim() ||
                              finalElement.getAttribute('data-cmt')?.split('_')[1] ||
                              finalElement.getAttribute('data-no');

                if (itemNo == String(banItem[0])) {
                    finalElement.classList.add('DCMOD_REDBG');
                }
            }

            if (item.type === 'post') {
                const aTags = finalElement.querySelectorAll('a');
                for (const aTag of aTags) {
                    const parentTd = aTag.closest('td');
                    if (parentTd && !parentTd.classList.contains('gall_tit')) {
                        aTag.removeAttribute('href');
                    }
                }
            }

            tbl.appendChild(finalElement);
        }

        const removedPosts = Array.from(currentDisplayedPosts.keys()).filter(pid =>
            !currentPostIds.has(pid)
        );

        for (const pid of removedPosts) {
            currentDisplayedPosts.delete(pid);
            processedContentIds.delete(pid);
            activeLoadPromises.delete(pid);
        }

        const removedComments = Array.from(currentDisplayedComments.keys()).filter(cid =>
            !currentCommentIds.has(cid)
        );

        for (const cid of removedComments) {
            currentDisplayedComments.delete(cid);
        }
    }

    async function buildInitialTable(allItems, ban_arr, tbl) {
        tbl.innerHTML = '';

        const gallList = document.querySelector('table.gall_list');
        if (gallList.querySelector('colgroup')) {
            gallList.querySelector('colgroup').innerHTML = `
                <col style="width:7%">
                <col style="width:51px">
                <col>
                <col style="width:18%">
                <col style="width:6%">
                <col style="width:6%">
                <col style="width:6%">`;
        }

        const chkboxhead = gallList.querySelector('thead th.chkbox_th');
        if (chkboxhead) chkboxhead.parentNode.removeChild(chkboxhead);

        for (const item of allItems) {
            const element = item.element;
            let itemId;

            if (item.type === 'post') {
                const pid = element.getAttribute('data-no');
                if (!pid) continue;
                itemId = pid;
                savePostState(element);
            } else if (item.type === 'comment') {
                const cid = element.getAttribute('data-cmt');
                if (!cid) continue;
                itemId = cid;
                saveCommentState(element);
            }

            for (const banItem of ban_arr) {
                const itemNo = element.querySelector('td.gall_num')?.textContent.trim() ||
                              element.getAttribute('data-cmt')?.split('_')[1] ||
                              element.getAttribute('data-no');

                if (itemNo == String(banItem[0])) {
                    element.classList.add('DCMOD_REDBG');
                }
            }

            if (item.type === 'post') {
                const aTags = element.querySelectorAll('a');
                for (const aTag of aTags) {
                    const parentTd = aTag.closest('td');
                    if (parentTd && !parentTd.classList.contains('gall_tit')) {
                        aTag.removeAttribute('href');
                    }
                }
            }

            tbl.appendChild(element);
        }
    }

    async function toggleGoogleSheetMonitoring() {
        if (googleSheetBanList.size === 0) {
            alert('갱차목록을 먼저 갱신해주세요.');
            return;
        }

        const btn = document.querySelector('#DCMOD_GOOGLESHEET_MONITOR_BTN');

        if (!googleSheetMonitoring) {
            googleSheetMonitoring = true;

            if (btn) {
                btn.textContent = '중지';
                btn.style.backgroundColor = '#dc3545';
            }

            backupOriginalTable();

            currentDisplayedPosts.clear();
            currentDisplayedComments.clear();
            processedContentIds.clear();
            realtimeLoadQueue.clear();
            activeLoadPromises.clear();
            firstMonitoringRun = true;

            googleSheetMonitor();
            monitoringInterval = setInterval(googleSheetMonitor, 5000);

        } else {
            googleSheetMonitoring = false;
            backgroundLoadingActive = false;
            realtimeLoadingActive = false;

            activeLoadPromises.clear();

            if (monitoringInterval) {
                clearInterval(monitoringInterval);
                monitoringInterval = null;
            }

            if (btn) {
                btn.style.backgroundColor = '#6c757d';
                btn.textContent = '중지중';
            }

            currentDisplayedPosts.clear();
            currentDisplayedComments.clear();
            processedContentIds.clear();
            realtimeLoadQueue.clear();

            restoreOriginalTable();
        }
    }

    function addButtons() {
        if (document.querySelector('#DCMOD_GOOGLESHEET_BTN')) return;

        const existingButton = document.querySelector('.DCMOD_SETTING_BTN') ||
                              document.querySelector('button') ||
                              document.querySelector('.btn');

        if (!existingButton) {
            setTimeout(addButtons, 1000);
            return;
        }

        const parentContainer = existingButton.parentNode || document.body;

        const updateBtn = document.createElement('button');
        updateBtn.id = 'DCMOD_GOOGLESHEET_BTN';
        updateBtn.className = 'DCMOD_SETTING_BTN';
        updateBtn.textContent = '목록갱신';
        updateBtn.style.cssText = `
            width: 54px;
            height: 24px;
            background-color: #007bff;
            color: white;
            border: none;
            border-radius: 4px;
            text-align: center;
            cursor: pointer;
            margin-left: 3px;
            font-size: 11px;
        `;
        updateBtn.onclick = fetchGoogleSheetData;
        parentContainer.appendChild(updateBtn);

        const monitorBtn = document.createElement('button');
        monitorBtn.id = 'DCMOD_GOOGLESHEET_MONITOR_BTN';
        monitorBtn.className = 'DCMOD_SETTING_BTN';
        monitorBtn.textContent = '모니터링';
        monitorBtn.style.cssText = `
            width: 54px;
            height: 24px;
            background-color: #007bff;
            color: white;
            border: none;
            border-radius: 4px;
            text-align: center;
            cursor: pointer;
            margin-left: 3px;
            font-size: 11px;
        `;
        monitorBtn.onclick = toggleGoogleSheetMonitoring;
        parentContainer.appendChild(monitorBtn);
    }

    function addStyles() {
        if (document.querySelector('#DCMOD_GOOGLESHEET_STYLE')) return;

        const style = document.createElement('style');
        style.id = 'DCMOD_GOOGLESHEET_STYLE';
        style.textContent = `
            .DCMOD_REDBG {
                background-color: #ffcccc !important;
            }

            #DCMOD_GOOGLESHEET_BTN:hover,
            #DCMOD_GOOGLESHEET_MONITOR_BTN:hover {
                opacity: 0.9;
                transform: translateY(-1px);
                transition: all 0.2s ease;
            }

            .DCMOD_SETTING_BTN {
                font-size: 11px !important;
                font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, sans-serif !important;
                line-height: 1.2 !important;
                text-overflow: ellipsis !important;
                overflow: hidden !important;
            }

            .post-content-preview {
                white-space: pre-wrap;
                word-break: break-word;
            }

            @keyframes realtimeContentLoad {
                0% {
                    opacity: 0;
                    transform: translateY(-10px);
                    border-left-color: #FF9800;
                }
                50% {
                    border-left-color: #FF9800;
                }
                100% {
                    opacity: 1;
                    transform: translateY(0);
                    border-left-color: #4CAF50;
                }
            }

            @keyframes fadeIn {
                from { opacity: 0; transform: translateY(-5px); }
                to { opacity: 1; transform: translateY(0); }
            }

            .post-content-preview img {
                max-width: 100% !important;
                height: auto !important;
                border-radius: 3px !important;
            }

            .DCMOD_DELETABLE {
                font-size: 11px;
                color: #666;
                margin-left: 5px;
            }

            .DCMOD_REDTEXT {
                color: #ff4444 !important;
                font-weight: bold;
            }

            .tooltip-text {
                visibility: hidden;
                background-color: #333;
                color: white;
                text-align: center;
                border-radius: 4px;
                padding: 5px;
                position: absolute;
                z-index: 1000;
                font-size: 11px;
                max-width: 200px;
                word-wrap: break-word;
            }

            .ub-writer:hover .tooltip-text {
                visibility: visible;
            }

            td.gall_tit a {
                color: inherit !important;
                text-decoration: none !important;
            }

            td.gall_tit a:hover {
                text-decoration: underline !important;
            }
        `;
        document.head.appendChild(style);
    }

    async function init() {
        if (!window.location.href.includes('gall.dcinside.com')) return;

        const saved = await GM.getValue('googleSheetBanList', []);
        if (saved.length > 0) {
            googleSheetBanList = new Set(saved);
        }

        addStyles();

        setTimeout(addButtons, 500);
        setTimeout(addButtons, 1000);
        setTimeout(addButtons, 2000);

        setTimeout(() => {
            process_ubwriter();
        }, 3000);
    }

    if (document.readyState === 'loading') {
        document.addEventListener('DOMContentLoaded', init);
    } else {
        init();
    }

})();