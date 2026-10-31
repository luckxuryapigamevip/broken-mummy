const WebSocket = require('ws');
const express = require('express');
const cors = require('cors');
const fs = require('fs');
const os = require('os');

const getNetworkInfo = () => {
    const interfaces = os.networkInterfaces();
    let localIP = '127.0.0.1';
    for (const ifaceName in interfaces) {
        for (const iface of interfaces[ifaceName]) {
            if (!iface.internal && iface.family === 'IPv4') {
                localIP = iface.address;
                break;
            }
        }
    }
    return { localIP };
};

const app = express();
app.use(cors());
const PORT = process.env.PORT || 3002;

let apiResponseData = {
    "phien": "Đang khởi tạo...",
    "tong_diem": 0,
    "ket_qua": "Đang chờ kết nối...",
    "thoi_gian_cap_nhat": "",
    "id": "@mrtinhios"
};

let currentSessionId = null;
let lastEventId = 19;
const patternHistory = [];

// ===================================
// LOGIN MESSAGE - Copy từ F12 WebSocket
// ===================================
const LOGIN_MESSAGE = [1, "MiniGame", "demohh87889", "12345678hk", { "info": "{\"ipAddress\":\"2001:ee0:452d:3f0:d64:1bce:65d7:7d8d\",\"wsToken\":\"eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9.eyJnZW5kZXIiOjAsImNhblZpZXdTdGF0IjpmYWxzZSwiZGlzcGxheU5hbWUiOiJsYWpoZ2hmIiwiYm90IjowLCJpc01lcmNoYW50IjpmYWxzZSwidmVyaWZpZWRCYW5rQWNjb3VudCI6ZmFsc2UsInBsYXlFdmVudExvYmJ5IjpmYWxzZSwiY3VzdG9tZXJJZCI6NjcwODUxMTUsImFmZklkIjoiNzg5IiwiYmFubmVkIjpmYWxzZSwiYnJhbmQiOiI3ODkuY2x1YiIsInRpbWVzdGFtcCI6MTc3MjA0MTE0Njg2OSwibG9ja0dhbWVzIjpbXSwiYW1vdW50IjowLCJsb2NrQ2hhdCI6ZmFsc2UsInBob25lVmVyaWZpZWQiOmZhbHNlLCJpcEFkZHJlc3MiOiIyMDAxOmVlMDo0NTJkOjNmMDpkNjQ6MWJjZTo2NWQ3OjdkOGQiLCJtdXRlIjpmYWxzZSwiYXZhdGFyIjoiaHR0cHM6Ly9hcGkueGV1aS5pby9pbWFnZXMvYXZhdGFyL2F2YXRhcl8xNy5wbmciLCJwbGF0Zm9ybUlkIjo0LCJ1c2VySWQiOiI2YzU3MGFkOS0zZWY4LTQ1YWYtYjcxOC0zMzY3Njg0M2ZjMzIiLCJyZWdUaW1lIjoxNzcyMDQxMTE3OTMzLCJwaG9uZSI6IiIsImRlcG9zaXQiOmZhbHNlLCJ1c2VybmFtZSI6IlM4X2RlbW9oaDg3ODg5In0.5dquWp8L8d2bYeUt5REhGxr6IFT15eXhG5qgWaG1uRw\",\"locale\":\"vi\",\"userId\":\"6c570ad9-3ef8-45af-b718-33676843fc32\",\"username\":\"S8_demohh87889\",\"timestamp\":1772041146869,\"refreshToken\":\"dee65492d8824b91a8903484aaa245c9.dde6dd2c8a894dac9cd106a929b8019d\"}", "signature": "3BD5C4F11511B3A828579CCFEF2F2A0CC5D2D19C7B5457A4A5BBA9DF56371E882C49EEB7EBB10F2D7C8B2D05305A52704F752D42107C5CECB887555331A1151F08F68F02F91C6152091E1DB9F8023BD07FAB3596FF9066520AD87EB860795638292742D812DBCE75EA99D284D8D93D57446818D96AADBA31ACC97A47153F122B" }];

// Subscribe commands
const SUBSCRIBE_TX = [6, "MiniGame", "taixiuUnbalancedPlugin", { "cmd": 2000 }];
const SUBSCRIBE_LOBBY = [6, "MiniGame", "lobbyPlugin", { "cmd": 10001 }];

const WEBSOCKET_URL = "wss://websocket.atpman.net/websocket";

const WS_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/143.0.0.0 Safari/537.36",
    "Origin": "https://play.789club.sh"
};

let ws = null;
let keepAliveInterval = null;
let reconnectTimeout = null;
let hasSubscribed = false; // Flag để ẩn log spam
let lastDataTime = Date.now(); // Watchdog đo thời gian nhận data cuối
const RECONNECT_DELAY = 3000;
const KEEPALIVE_INTERVAL = 10000; // Phải là 10 giây để server không drop sub

function connectWebSocket() {
    console.log(`\n[🔄] Đang nối dây tới mạng 789Club...`);

    // Dọn dẹp kết nối cũ
    clearInterval(keepAliveInterval);
    clearTimeout(reconnectTimeout);
    keepAliveInterval = null;

    if (ws) {
        ws.removeAllListeners();
        ws.close();
    }

    ws = new WebSocket(WEBSOCKET_URL, {
        headers: WS_HEADERS,
        rejectUnauthorized: false
    });

    ws.on('open', () => {
        console.log('[✅] WebSocket connected to 789Club');

        // Gửi login + subscribe lần lượt (giống Sun.Win initialMessages)
        const initMessages = [LOGIN_MESSAGE, SUBSCRIBE_TX, SUBSCRIBE_LOBBY];
        initMessages.forEach((msg, i) => {
            setTimeout(() => {
                if (ws && ws.readyState === WebSocket.OPEN) {
                    ws.send(JSON.stringify(msg));
                    if (i === 0) console.log('[📤] Đã gửi gói đăng nhập');
                    if (i === 1) console.log('[📤] Đăng ký nhận kết quả Tài/Xỉu (cmd: 2000)');
                    if (i === 2) console.log('[📤] Đăng ký nhận thông tin Lobby (cmd: 10001)');
                }
            }, i * 600);
        });

        // ============================================================
        // KEEPALIVE - ĐÃ THÊM LẠI RE-SUBSCRIBE & WATCHDOG
        // Server 789Club CẦN gửi lại subscribe mỗi 10s, nếu không nó 
        // sẽ hủy đăng ký (drop subscription) và im lặng sau vài phiên
        // ============================================================
        clearInterval(keepAliveInterval);
        keepAliveInterval = setInterval(() => {
            if (ws && ws.readyState === WebSocket.OPEN) {
                // WATCHDOG: Chống treo khi login đè trên web
                // Bị im lặng quá 60 giây -> Tự sát để giật lại kết nối
                if (Date.now() - lastDataTime > 60000) {
                    console.log(`\n[⚠️] CHÓ GÁC CỔNG: Bị treo không có dữ liệu quá 60 giây (Có thể do login trùng). Cưỡng chế Reconnect!\n`);
                    ws.terminate(); // Đóng kết nối lập tức, kích hoạt ws.on('close')
                    return;
                }

                // 1. TCP-level ping
                ws.ping();
                // 2. App-level ping "2"
                ws.send("2");
                // 3. BẮT BUỘC: Đăng ký lại Tài Xỉu liên tục để k bị drop
                ws.send(JSON.stringify(SUBSCRIBE_TX));
                // 4. Đồng bộ event Simms
                ws.send(JSON.stringify([7, "Simms", lastEventId, 0, { "id": 0 }]));
            }
        }, KEEPALIVE_INTERVAL);
    });

    ws.on('pong', () => {
        // TCP keepalive OK - connection stable
    });

    // ============================================================
    // MESSAGE HANDLER - MÔ HÌNH STREAM REALTIME NHƯ SUN.WIN
    // 
    // Nguyên tắc:
    //   1. KHÔNG filter sid → log MỌI event
    //   2. KHÔNG dùng flag chặn → nhận là xử lý
    //   3. KHÔNG return sớm trong game event → stream liên tục
    //   4. currentSessionId chỉ dùng để HIỂN THỊ, không dùng để FILTER
    //
    // Luồng game:
    //   cmd 2000 → subscribe OK (chỉ log 1 lần đầu)
    //   cmd 2007 → update cược → LOG LIÊN TỤC phiên hiện tại
    //   cmd 2006 → KẾT QUẢ xúc xắc → LOG NGAY
    //   cmd 2005 → phiên mới → CẬP NHẬT sid + LOG
    // ============================================================
    ws.on('message', (data) => {
        const messageStr = Buffer.isBuffer(data) ? data.toString('utf8') : data;

        // Lưu raw message vào file debug
        fs.appendFileSync('dice_789.txt', messageStr + '\n');

        try {
            // Bỏ qua message không phải JSON array
            if (!messageStr.startsWith('[')) return;

            const parsed = JSON.parse(messageStr);
            const opcode = parsed[0];

            // ---- Cập nhật Simms EventID ----
            if (opcode === 7 && parsed[1] === "Simms" && typeof parsed[2] === 'number') {
                lastEventId = parsed[2];
                return;
            }

            // ---- Bỏ qua các opcode hệ thống ----
            if (opcode === 0) return; // ping response
            if (opcode === 2) return; // ack cho "2" ping

            // ---- Re-auth ----
            if (opcode === 1 && parsed[1] === true) {
                console.log(`[🔑] Server refresh session OK`);
                return;
            }

            // ---- Chỉ xử lý GAME DATA (opcode 5) ----
            if (opcode !== 5 || !parsed[1] || typeof parsed[1] !== 'object') return;

            const gameData = parsed[1];
            const cmd = gameData.cmd;

            // Reset Watchdog vì đã nhận được gói tin Game (chứng tỏ server còn trả data)
            lastDataTime = Date.now();

            // Bỏ qua noise
            if (cmd === 10000 || cmd === 2008) return;

            // ---- CMD 100: Login OK ----
            if (cmd === 100) {
                console.log(`🔄 Đăng nhập OK | User: ${gameData.u} | UID: ${gameData.uid}`);
                return;
            }

            // ---- CMD 10001: Lobby info ----
            if (cmd === 10001) {
                console.log(`[🏛️] Nhận thông tin lobby`);
                return;
            }

            // ============================================================
            // ⬇️ TỪ ĐÂY TRỞ XUỐNG: GAME EVENTS - STREAM LIÊN TỤC 24/7
            // ============================================================

            // ---- CMD 2000: Subscribe response ----
            // Nhận liên tục mỗi 10s do keepalive, nhưng CHỈ LOG 1 LẦN để chống rác
            if (cmd === 2000) {
                currentSessionId = gameData.sid;

                if (!hasSubscribed) {
                    hasSubscribed = true;
                    console.log(`[🎮] Subscribe OK → Phiên: ${gameData.sid} | State: ${gameData.gS} | Còn: ${gameData.rmT}ms`);

                    if (gameData.htr && gameData.htr.length > 0) {
                        const last = gameData.htr[gameData.htr.length - 1];
                        const t = last.d1 + last.d2 + last.d3;
                        console.log(`[📜] Lịch sử gần nhất: Phiên ${last.sid} → ${last.d1}+${last.d2}+${last.d3}=${t} (${t > 10 ? 'TÀI' : 'XỈU'})`);
                    }
                }
                // KHÔNG return → tiếp tục nhận event
            }

            // ---- CMD 2005: PHIÊN MỚI ----
            // Cập nhật currentSessionId NGAY LẬP TỨC
            else if (cmd === 2005) {
                currentSessionId = gameData.sid;
                apiResponseData.phien = gameData.sid.toString();
                apiResponseData.ket_qua = "Đang chờ xổ...";
                console.log(`[🆕] ═══ PHIÊN MỚI: ${gameData.sid} ═══`);
            }

            // ---- CMD 2006: 🎲🎲🎲 KẾT QUẢ XÚC XẮC 🎲🎲🎲 ----
            // KHÔNG filter sid, KHÔNG check bất kỳ điều kiện nào
            // Nhận là log NGAY LẬP TỨC
            else if (cmd === 2006) {
                const { d1, d2, d3, sid } = gameData;

                if (d1 === undefined || d2 === undefined || d3 === undefined) {
                    console.log(`[⚠️] cmd:2006 thiếu dice data:`, JSON.stringify(gameData).substring(0, 300));
                } else {
                    const score = d1 + d2 + d3;
                    const result = (score > 10) ? "TÀI" : "XỈU";
                    const curTime = new Date();
                    const timeString = curTime.toLocaleTimeString('vi-VN', { hour12: false }) + " - " + curTime.toLocaleDateString('vi-VN');

                    // Cập nhật sid (phòng trường hợp 2005 bị miss)
                    currentSessionId = sid || currentSessionId;

                    apiResponseData = {
                        "phien": (sid || currentSessionId || "N/A").toString(),
                        "tong_diem": score,
                        "ket_qua": result,
                        "thoi_gian_cap_nhat": timeString,
                        "id": "@mrtinhios",
                        "update_count": (apiResponseData.update_count || 0) + 1
                    };

                    // Lưu lịch sử (giống Sun.Win)
                    patternHistory.push({
                        session: sid || currentSessionId,
                        dice: [d1, d2, d3],
                        total: score,
                        result: result,
                        timestamp: curTime.toISOString()
                    });
                    if (patternHistory.length > 100) patternHistory.shift();

                    console.log(`[🎲] Phiên ${sid}: Tổng điểm: ${score} (${result})`);
                }
            }

            // ---- CMD 2007: Cập nhật tiền cược ----
            // LOG LIÊN TỤC phiên hiện tại → ĐÂY LÀ HEARTBEAT CỦA STREAM
            // KHÔNG filter theo currentSessionId !== gameData.sid
            // Mỗi event 2007 = phiên đang sống → log ra để thấy stream chạy
            else if (cmd === 2007 && gameData.sid) {
                currentSessionId = gameData.sid;
                apiResponseData.phien = gameData.sid.toString();
                console.log(`[🎮] Phiên mới: ${gameData.sid}`);
            }

            // ---- CMD không xác định → LOG ĐẦY ĐỦ để debug ----
            else if (cmd && ![10000, 10001, 2008, 2000, 2005, 2006, 2007].includes(cmd)) {
                console.log(`[❓] CMD chưa biết: ${cmd}`);
                console.log(`     Data:`, JSON.stringify(gameData).substring(0, 400));
            }

        } catch (error) {
            if (!messageStr.startsWith('3')) {
                fs.appendFileSync('error_789_log.txt', `[${new Date().toISOString()}] ${error.message} | msg: ${messageStr.substring(0, 100)}\n`);
            }
        }
    });

    ws.on('close', (code, reason) => {
        console.log(`[❌] WebSocket đóng! Mã: ${code} | Lý do: ${reason || 'N/A'}`);
        console.log(`[🔄] Reconnect sau ${RECONNECT_DELAY / 1000}s...`);
        hasSubscribed = false; // Reset lại khi reconnect
        clearInterval(keepAliveInterval);
        keepAliveInterval = null;
        apiResponseData.ket_qua = "Mất tín hiệu. Đang nối lại...";
        clearTimeout(reconnectTimeout);
        reconnectTimeout = setTimeout(connectWebSocket, RECONNECT_DELAY);
    });

    ws.on('error', (err) => {
        console.log('[💥] Lỗi WebSocket:', err.message);
        ws.close();
    });
}

// ============================================================
// HTTP API
// ============================================================
app.get('/', (req, res) => res.json(apiResponseData));
app.get('/api/ditmemaysun', (req, res) => res.json(apiResponseData));

app.get('/api/history', (req, res) => {
    res.json({
        current: apiResponseData,
        history: patternHistory.slice(-20),
        total_requests: apiResponseData.update_count || 0
    });
});

app.get('/api/stats', (req, res) => {
    const taiCount = patternHistory.filter(item => item.result === "TÀI").length;
    const xiuCount = patternHistory.filter(item => item.result === "XỈU").length;
    res.json({
        total_sessions: patternHistory.length,
        tai_count: taiCount,
        xiu_count: xiuCount,
        tai_percentage: patternHistory.length > 0 ? ((taiCount / patternHistory.length) * 100).toFixed(2) : 0,
        xiu_percentage: patternHistory.length > 0 ? ((xiuCount / patternHistory.length) * 100).toFixed(2) : 0,
        last_update: apiResponseData.thoi_gian_cap_nhat,
        server_uptime: process.uptime().toFixed(0) + 's'
    });
});

app.get('/api/health', (req, res) => {
    res.json({
        status: 'online',
        websocket: ws ? ws.readyState === WebSocket.OPEN : false,
        uptime: process.uptime(),
        memory: process.memoryUsage(),
        connections: ws ? 'connected' : 'disconnected'
    });
});

app.listen(PORT, '0.0.0.0', () => {
    const networkInfo = getNetworkInfo();
    console.log(`\n${'═'.repeat(50)}`);
    console.log(`🚀 WORM GPT 789Club Data Stream`);
    console.log(`${'═'.repeat(50)}`);
    console.log(`📡 Server:`);
    console.log(`   Local:   http://localhost:${PORT}`);
    console.log(`   Network: http://${networkInfo.localIP}:${PORT}`);
    console.log(`${'═'.repeat(50)}`);
    console.log(`🔌 Connecting to 789Club...`);
    console.log(`${'═'.repeat(50)}\n`);

    connectWebSocket();
});
