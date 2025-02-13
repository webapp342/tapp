const TelegramBot = require('node-telegram-bot-api');
const admin = require('firebase-admin');
const path = require('path');
const chalk = require('chalk');
const { spawn } = require('child_process');

// Memory cache for users
const userCache = new Map();
const CACHE_DURATION = 60 * 60 * 1000; // 1 hour
const RATE_LIMIT_DURATION = 1000; // 1 second
const commandRateLimit = new Map();

// Global users map
const usersMap = new Map();

// Invite counter for batch processing
const inviteCounter = new Map();
const INVITE_BATCH_LIMIT = 100;

// lastAdReward değişiklik sayacı
let lastAdRewardChangeCount = 0;
let totalAdRewardChangeCount = 0;

// Admin document ID
const ADMIN_DOC_ID = 'admin_stats';

// Logging utility functions
const log = {
    info: (msg) => console.log(chalk.blue('ℹ INFO:'), msg),
    success: (msg) => console.log(chalk.green('✔ SUCCESS:'), msg),
    warning: (msg) => console.log(chalk.yellow('⚠ WARNING:'), msg),
    error: (msg) => console.log(chalk.red('✖ ERROR:'), msg),
    user: (msg) => console.log(chalk.magenta('👤 USER:'), msg),
    system: (msg) => console.log(chalk.cyan('🔧 SYSTEM:'), msg)
};

// Cache functions
function getCachedUser(userId) {
    const cached = userCache.get(userId);
    if (cached && Date.now() - cached.timestamp < CACHE_DURATION) {
        return cached.data;
    }
    return null;
}

function setCachedUser(userId, userData) {
    userCache.set(userId, {
        data: userData,
        timestamp: Date.now()
    });
}

// Rate limit check
function checkRateLimit(userId, command) {
    const key = `${userId}:${command}`;
    const lastUsage = commandRateLimit.get(key);
    const now = Date.now();

    if (lastUsage && now - lastUsage < RATE_LIMIT_DURATION) {
        return false;
    }

    commandRateLimit.set(key, now);
    return true;
}

// Load all users into memory
async function loadAllUsers(db) {
    try {
        log.system('Loading all users into memory...');
        const snapshot = await db.collection('users').get();
        snapshot.forEach(doc => {
            const userData = doc.data();
            usersMap.set(String(userData.userId), userData);
        });
        log.success(`Loaded ${usersMap.size} users into memory`);

        // Admin koleksiyonunu ve dokümanını oluştur/yükle
        try {
            const adminRef = db.collection('admin').doc(ADMIN_DOC_ID);
            const adminDoc = await adminRef.get();

            if (!adminDoc.exists) {
                // Admin dokümanı yoksa oluştur
                await adminRef.set({
                    totalAdRewardChangeCount: 0,
                    lastUpdated: Date.now(),
                    createdAt: Date.now()
                });
                totalAdRewardChangeCount = 0;
                log.success('Created admin document with initial values');
                log.info(`Initial Total Ad Reward Changes: ${totalAdRewardChangeCount}`);
            } else {
                // Varolan değerleri yükle
                const adminData = adminDoc.data();
                totalAdRewardChangeCount = adminData.totalAdRewardChangeCount || 0;
                log.success(`Loaded totalAdRewardChangeCount: ${totalAdRewardChangeCount}`);
                log.info(`Current Total Ad Reward Changes: ${totalAdRewardChangeCount}`);
            }
        } catch (error) {
            log.error(`Failed to setup admin document: ${error.message}`);
            // Admin dokümanı oluşturulamadı/yüklenemedi, varsayılan değerlerle devam et
            totalAdRewardChangeCount = 0;
        }

    } catch (error) {
        log.error(`Failed to load users or admin stats: ${error.message}`);
        throw error;
    }
}

// Auto-restart function
function startBot() {
    try {
        // Firebase başlatma kontrolü
        if (!admin.apps.length) {
            // Initialize Firebase
            const serviceAccount = require('./telegram-cc828-firebase-adminsdk-noq3l-85707547f8.json');
            admin.initializeApp({
                credential: admin.credential.cert(serviceAccount),
                databaseURL: "https://boobablip.firebaseio.com"
            });
            log.system('Firebase initialized successfully');
        } else {
            log.system('Firebase already initialized, using existing instance');
        }

        // Firestore reference
        const db = admin.firestore();

        // Load all users first
        loadAllUsers(db).then(() => {
            // Initialize Telegram bot
            const token = '7807923003:AAFEU8emCnRARV5QyuzPTdZwTzWxeI04qcU';
            const bot = new TelegramBot(token, { polling: true });

            // Start lastAdReward listener
            function startAdRewardListener(db) {
                log.system('Starting lastAdReward listener...');

                // RAM'deki lastAdReward değerlerini periyodik olarak kontrol et
                setInterval(() => {
                    const now = Date.now();
                    const oneHourInMs = 60 * 60 * 1000;

                    // Tüm kullanıcıları kontrol et
                    for (const [userId, userData] of usersMap.entries()) {
                        if (userData.lastAdReward && now - userData.lastAdReward >= oneHourInMs) {
                            // Son bildirimden en az 6 saat geçtiyse yeni bildirim gönder
                            const sixHoursInMs = 6 * 60 * 60 * 1000;
                            if (!userData.lastAdNotification || now - userData.lastAdNotification >= sixHoursInMs) {
                                const message = "🎰 Your Free Spin is ready! Don't miss your chance to win big!";

                                bot.sendMessage(userId, message, {
                                    reply_markup: {
                                        inline_keyboard: [[
                                            { text: "🎯 Spin Now!", web_app: { url: "https://app.bblip.io/" }}
                                        ]]
                                    }
                                }).then(() => {
                                    userData.lastAdNotification = now;
                                    usersMap.set(userId, userData);
                                    log.success(`Sent periodic free spin notification to user ${userId}`);
                                }).catch(error => {
                                    log.error(`Failed to send periodic notification to user ${userId}: ${error.message}`);
                                });
                            }
                        }
                    }
                }, 5 * 60 * 1000); // Her 5 dakikada bir kontrol et

                // Firestore listener for lastAdReward changes
                const unsubscribe = db.collection('users')
                    .where('lastAdReward', '>', 0)
                    .onSnapshot((snapshot) => {
                        snapshot.docChanges().forEach((change) => {
                            if ((change.type === 'modified' || change.type === 'added') && change.doc.data().lastAdReward) {
                                const userData = change.doc.data();
                                const userId = String(userData.userId);
                                const oldData = change.type === 'modified' ? change.doc.oldData() : null;

                                // Sadece lastAdReward değişmişse işlem yap
                                if (change.type === 'modified' && oldData && userData.lastAdReward === oldData.lastAdReward) {
                                    return; // lastAdReward değişmemişse işlem yapma
                                }

                                // lastAdReward değişiklik sayacını artır
                                lastAdRewardChangeCount++;
                                totalAdRewardChangeCount++;

                                // Toplam değişiklik sayısını logla
                                log.info(`Total Ad Reward Changes: ${totalAdRewardChangeCount}`);

                                // RAM'deki kullanıcıyı güncelle
                                const existingUser = usersMap.get(userId);
                                if (existingUser) {
                                    existingUser.lastAdReward = userData.lastAdReward;
                                    usersMap.set(userId, existingUser);
                                    log.info(`Updated lastAdReward for user ${userId}: ${userData.lastAdReward}`);
                                }
                            }
                        });
                    }, (error) => {
                        log.error(`Error in lastAdReward listener: ${error.message}`);
                        unsubscribe();
                        setTimeout(() => {
                            log.info('Attempting to restart lastAdReward listener...');
                            startAdRewardListener(db);
                        }, 5000);
                    });

                log.success('lastAdReward listener and periodic check system started successfully');
            }

            // Start the listener
            startAdRewardListener(db);

            // Bot error handling
            bot.on('polling_error', (error) => {
                log.error(`Polling error: ${error.message}`);
                restartBot();
            });

            bot.on('error', (error) => {
                log.error(`Bot error: ${error.message}`);
                restartBot();
            });

            bot.on('webhook_error', (error) => {
                log.error(`Webhook error: ${error.message}`);
                restartBot();
            });

            log.system('Telegram bot started and listening for messages');

            // Health check
            let lastHealthCheck = Date.now();
            const healthCheckInterval = setInterval(() => {
                const now = Date.now();
                if (now - lastHealthCheck > 310000) { // 5 minutes + 10 seconds tolerance
                    log.warning('Health check failed - restarting bot');
                    clearInterval(healthCheckInterval);
                    restartBot();
                }
                lastHealthCheck = now;
                log.info('Bot health check: Running');
            }, 300000); // Every 5 minutes

            // Admin broadcast command
            bot.onText(/\/broadcast (.+)/, async (msg, match) => {
                try {
                    const adminId = msg.from.id;
                    const adminUsers = [7046348699]; // Buraya kendi Telegram ID'nizi ekleyin

                    if (!adminUsers.includes(adminId)) {
                        log.warning(`Unauthorized broadcast attempt from user ${adminId}`);
                        bot.sendMessage(msg.chat.id, "You are not authorized to use this command.");
                        return;
                    }

                    const broadcastMessage = match[1];
                    log.info(`Starting broadcast: "${broadcastMessage}"`);

                    // Get all users from Firestore
                    const usersSnapshot = await db.collection('users').get();
                    let successCount = 0;
                    let failCount = 0;
                    let blockedCount = 0;
                    let inactiveCount = 0;

                    bot.sendMessage(msg.chat.id, `Starting broadcast to ${usersSnapshot.size} users...`);

                    // Chunk users into groups of 30 to avoid rate limits
                    const CHUNK_SIZE = 30;
                    const DELAY_BETWEEN_CHUNKS = 1000; // 1 second delay between chunks
                    const chunks = [];

                    for (let i = 0; i < usersSnapshot.docs.length; i += CHUNK_SIZE) {
                        chunks.push(usersSnapshot.docs.slice(i, i + CHUNK_SIZE));
                    }

                    for (const chunk of chunks) {
                        await Promise.all(chunk.map(async (doc) => {
                            const userData = doc.data();
                            try {
                                await bot.sendMessage(userData.userId, broadcastMessage, {
                                    parse_mode: 'HTML',
                                    disable_web_page_preview: true
                                });

                                // Sadece RAM'de lastActive güncelleme
                                const user = usersMap.get(String(userData.userId));
                                if (user) {
                                    user.lastActive = Date.now();
                                    usersMap.set(String(userData.userId), user);
                                }

                                successCount++;
                                if (successCount % 10 === 0) {
                                    log.success(`Broadcast progress: ${successCount}/${usersSnapshot.size}`);
                                }
                            } catch (error) {
                                failCount++;
                                if (error.message.includes('blocked') || error.message.includes('bot was blocked')) {
                                    blockedCount++;
                                    // Mark user as blocked in database
                                    await db.collection('users').doc(String(userData.userId)).update({
                                        isBlocked: true,
                                        lastBlockedAt: Date.now()
                                    });
                                    log.warning(`User ${userData.userId} has blocked the bot`);
                                } else if (error.message.includes('chat not found') || error.message.includes('user is deactivated')) {
                                    inactiveCount++;
                                    // Mark user as inactive
                                    await db.collection('users').doc(String(userData.userId)).update({
                                        isActive: false,
                                        lastInactiveAt: Date.now()
                                    });
                                    log.warning(`User ${userData.userId} is inactive or has deleted their account`);
                                } else {
                                    log.error(`Failed to send broadcast to user ${userData.userId}: ${error.message}`);
                                }
                            }
                        }));

                        await new Promise(resolve => setTimeout(resolve, DELAY_BETWEEN_CHUNKS));
                    }

                    const summary = `📬 Broadcast Summary:\n\n` +
                                   `✅ Successfully Sent: ${successCount}\n` +
                                   `❌ Failed: ${failCount}\n` +
                                   `🚫 Blocked Bot: ${blockedCount}\n` +
                                   `⚪ Inactive Users: ${inactiveCount}\n` +
                                   `📊 Total Attempted: ${usersSnapshot.size}`;

                    log.info(summary);
                    bot.sendMessage(msg.chat.id, summary);

                    // If there were blocked users, offer to clean up the database
                    if (blockedCount > 0) {
                        bot.sendMessage(msg.chat.id, 
                            `There are ${blockedCount} users who have blocked the bot.\n` +
                            `Use /cleanup to remove blocked and inactive users from the database.`
                        );
                    }

                } catch (error) {
                    log.error(`Broadcast error: ${error.message}`);
                    bot.sendMessage(msg.chat.id, `Error during broadcast: ${error.message}`);
                }
            });

            // Cleanup command for removing blocked/inactive users
            bot.onText(/\/cleanup/, async (msg) => {
                try {
                    const adminId = msg.from.id;
                    const adminUsers = [7046348699]; // Updated admin ID

                    if (!adminUsers.includes(adminId)) {
                        log.warning(`Unauthorized cleanup attempt from user ${adminId}`);
                        bot.sendMessage(msg.chat.id, "You are not authorized to use this command.");
                        return;
                    }

                    const usersSnapshot = await db.collection('users').get();
                    let removedCount = 0;
                    let totalUsers = usersSnapshot.size;

                    bot.sendMessage(msg.chat.id, `Starting database cleanup...`);

                    for (const doc of usersSnapshot.docs) {
                        const userData = doc.data();
                        if (userData.isBlocked || userData.isActive === false) {
                            await db.collection('users').doc(doc.id).delete();
                            removedCount++;
                            log.info(`Removed user ${userData.userId} from database`);
                        }
                    }

                    const summary = `🧹 Cleanup Summary:\n\n` +
                                   `🗑️ Removed Users: ${removedCount}\n` +
                                   `👥 Remaining Users: ${totalUsers - removedCount}\n` +
                                   `📊 Total Before Cleanup: ${totalUsers}`;

                    log.success(summary);
                    bot.sendMessage(msg.chat.id, summary);

                } catch (error) {
                    log.error(`Cleanup error: ${error.message}`);
                    bot.sendMessage(msg.chat.id, `Error during cleanup: ${error.message}`);
                }
            });

            // Stats command for admins
            bot.onText(/\/stats/, async (msg) => {
                try {
                    const adminId = msg.from.id;
                    const adminUsers = [7046348699]; // Updated admin ID

                    if (!adminUsers.includes(adminId)) {
                        log.warning(`Unauthorized stats request from user ${adminId}`);
                        bot.sendMessage(msg.chat.id, "You are not authorized to use this command.");
                        return;
                    }

                    const usersSnapshot = await db.collection('users').get();
                    const totalUsers = usersSnapshot.size;

                    let totalBblip = 0;
                    let activeUsers = 0;
                    const last24h = Date.now() - (24 * 60 * 60 * 1000);

                    usersSnapshot.forEach(doc => {
                        const userData = doc.data();
                        totalBblip += userData.bblip || 0;
                        if (userData.lastActive && userData.lastActive > last24h) {
                            activeUsers++;
                        }
                    });

                    const stats = `📊 Bot Statistics\n\n` +
                                `👥 Total Users: ${totalUsers}\n` +
                                `🟢 Active Users (24h): ${activeUsers}\n` +
                                `💎 Total BBliP: ${totalBblip}`;

                    log.info(`Stats requested by admin ${adminId}`);
                    bot.sendMessage(msg.chat.id, stats);

                } catch (error) {
                    log.error(`Stats error: ${error.message}`);
                    bot.sendMessage(msg.chat.id, `Error getting stats: ${error.message}`);
                }
            });

            // User engagement system
            async function startUserEngagement(userId, username, chatId) {
                const intervals = {
                    INSTANT: 0,           // Hemen
                    MINUTE_5: 5 * 60000,  // 5 dakika sonra
                    MINUTE_30: 30 * 60000,// 30 dakika sonra
                    HOUR_2: 2 * 3600000,  // 2 saat sonra
                    HOUR_12: 12 * 3600000,// 12 saat sonra
                    HOUR_24: 24 * 3600000,// 24 saat sonra
                    HOUR_36: 36 * 3600000 // 36 saat sonra
                };

                const messages = [
                    {
                        delay: intervals.INSTANT,
                        message: `Hello ${username}! 🎉 Welcome to BoobaBlip!`,
                        keyboard: {
                            inline_keyboard: [[{ text: "Launch Booba", web_app: { url: "https://app.bblip.io/" }}]]
                        }
                    },
                    {
                        delay: intervals.MINUTE_5,
                        message: "🎁 Pro Tip: Complete daily tasks to earn BBliP faster!\n\nTry your first task now?"
                    },
                    {
                        delay: intervals.MINUTE_30,
                        message: "🌟 You're doing great! Did you know you can earn bonus BBliP by inviting friends?\n\nUse your unique invite link to start earning!"
                    },
                    {
                        delay: intervals.HOUR_2,
                        message: "⭐ Hey! Your first daily reward is ready to collect!\n\nDon't miss out on your BBliP bonus!"
                    },
                    {
                        delay: intervals.HOUR_12,
                        message: "🎯 Your progress is impressive! Ready for a new challenge?\n\nComplete today's special mission for 2x BBliP!"
                    },
                    {
                        delay: intervals.HOUR_24,
                        message: "🎊 It's been 24 hours since you joined!\n\nClaim your special 24-hour milestone reward now!"
                    },
                    {
                        delay: intervals.HOUR_36,
                        message: "💫 Don't forget to check in daily for rewards!\n\nYour next big bonus is just around the corner!"
                    }
                ];

                messages.forEach(async (msg) => {
                    setTimeout(async () => {
                        try {
                            const userData = usersMap.get(String(userId));
                            if (!userData) return;

                            if (msg.keyboard) {
                                await bot.sendMessage(chatId, msg.message, {
                                    reply_markup: msg.keyboard
                                });
                            } else {
                                await bot.sendMessage(chatId, msg.message);
                            }

                            log.success(`Engagement message sent to ${username} (${userId}) at ${new Date().toISOString()}`);
                        } catch (error) {
                            log.error(`Failed to send engagement message to ${username}: ${error.message}`);
                        }
                    }, msg.delay);
                });
            }

            // Admin notification function
            function notifyAdmin(message) {
                const adminId = 7046348699;
                try {
                    bot.sendMessage(adminId, message);
                } catch (error) {
                    log.error(`Failed to notify admin: ${error.message}`);
                }
            }

            // Modify /start command
            bot.onText(/\/start(?: (.+))?/, async (msg, match) => {
                try {
                    const chatId = msg.chat.id;
                    const username = msg.from.username || 'Unknown';
                    const userId = msg.from.id;
                    const inviterId = match[1];

                    // Detailed logging of start command
                    log.user(`👤 User @${username} (${userId}) triggered /start command`);

                    // Rate limit check
                    if (!checkRateLimit(userId, 'start')) {
                        log.warning(`Rate limit hit for user @${username} (${userId}) on /start command`);
                        return;
                    }

                    // Check if user exists in memory
                    const existingUser = usersMap.get(String(userId));
                    const userRef = db.collection('users').doc(String(userId));

                    if (!existingUser) {
                        log.info(`New user registration: @${username} (${userId})`);

                        // New user
                        const newUserData = {
                            userId: userId,
                            username: username,
                            comment: `${userId}`,
                            amount: 0,
                            points: 0,
                            bblip: 0,
                            total: 0,
                            tickets: 0,
                            keys: 0,
                            keyParts: 0,
                            giftBox: 0,
                            invitedBy: inviterId || null,
                            inviteLink: `https://t.me/BoobaBlipBot?start=${userId}`,
                            createdAt: Date.now(),
                            lastActive: Date.now()  // lastActive sadece RAM için
                        };

                        // Update memory and database (lastActive hariç)
                        usersMap.set(String(userId), newUserData);
                        const dbData = {...newUserData};
                        delete dbData.lastActive;  // Veritabanına lastActive'i kaydetmiyoruz
                        await userRef.set(dbData);
                        log.success(`User data saved: @${username} (${userId})`);

                        // Notify admin about new user
                        const totalUsers = usersMap.size;
                        const adminMessage = `👤 New User Registered:\n` +
                                           `Username: @${username}\n` +
                                           `User ID: ${userId}\n` +
                                           `Invited By: ${inviterId || 'Direct'}\n\n` +
                                           `📊 Total Users: ${totalUsers}\n` +
                                           `⏰ Time: ${new Date().toLocaleString()}`;

                        notifyAdmin(adminMessage);
                        log.info(`Admin notified about new user: @${username}`);

                        // Start engagement system for new user
                        startUserEngagement(userId, username, chatId);
                        log.info(`Started engagement system for @${username}`);

                        // Handle inviter directly
                        if (inviterId) {
                            const inviterRef = db.collection('users').doc(String(inviterId));
                            const inviterData = usersMap.get(String(inviterId));

                            if (inviterData) {
                                await inviterRef.update({
                                    bblip: admin.firestore.FieldValue.increment(1000)
                                });
                                inviterData.bblip += 1000;
                                usersMap.set(String(inviterId), inviterData);
                                log.success(`Updated inviter @${inviterData.username} (${inviterId}) BBliP: +1000`);

                                bot.sendMessage(
                                    inviterId,
                                    `Congratulations! A new user has registered. You earned +10 BBliP!`
                                );
                                log.info(`Sent bonus notification to inviter @${inviterData.username}`);
                            } else {
                                log.warning(`Inviter not found for ID: ${inviterId}`);
                            }
                        }
                    } else {
                        log.info(`Existing user return: @${username} (${userId})`);

                        // Sadece RAM'de lastActive güncelleme
                        existingUser.lastActive = Date.now();
                        usersMap.set(String(userId), existingUser);
                        log.success(`Updated last active time in RAM for @${username}`);

                        bot.sendMessage(chatId, `Welcome back to BoobaBlip! 🎉`, {
                            reply_markup: {
                                inline_keyboard: [
                                    [{
                                        text: "Launch Booba",
                                        web_app: { url: "https://app.bblip.io/" }
                                    }]
                                ]
                            }
                        });
                        log.info(`Sent welcome back message to @${username}`);
                    }
                } catch (error) {
                    const errorMsg = `❌ Error in /start command:\n${error.message}`;
                    notifyAdmin(errorMsg);
                    log.error(`Error processing /start for user ${msg.from.username} (${msg.from.id}): ${error.message}`);
                }
            });

            // Add 2-hour lastAdReward statistics notification
            setInterval(async () => {
                try {
                    // Admin dokümanını güncelle
                    const adminRef = db.collection('admin').doc(ADMIN_DOC_ID);

                    // Transaction ile güvenli güncelleme yap
                    await db.runTransaction(async (transaction) => {
                        const adminDoc = await transaction.get(adminRef);
                        if (!adminDoc.exists) {
                            // Doküman yoksa oluştur
                            transaction.set(adminRef, {
                                totalAdRewardChangeCount: totalAdRewardChangeCount,
                                lastUpdated: Date.now(),
                                createdAt: Date.now()
                            });
                        } else {
                            // Varolan dokümanı güncelle
                            transaction.update(adminRef, {
                                totalAdRewardChangeCount: totalAdRewardChangeCount,
                                lastUpdated: Date.now()
                            });
                        }
                    });

                    log.success(`Saved totalAdRewardChangeCount to admin document: ${totalAdRewardChangeCount}`);

                    notifyAdmin(
                        `📊 Ad Reward Statistics:\n\n` +
                        `🎯 Changes in Last 2 Hours: ${lastAdRewardChangeCount}\n` +
                        `📈 Total Changes Since Start: ${totalAdRewardChangeCount}\n` +
                        `⏰ Time: ${new Date().toLocaleString()}`
                    );

                    // Sadece 2 saatlik sayacı sıfırla
                    lastAdRewardChangeCount = 0;

                } catch (error) {
                    log.error(`Failed to update admin stats: ${error.message}`);
                }
            }, 2 * 3600000); // Her 2 saatte bir

            // Add hourly statistics notification
            setInterval(async () => {
                try {
                    const totalUsers = usersMap.size;
                    const last24h = Date.now() - (24 * 60 * 60 * 1000);
                    const activeUsers = Array.from(usersMap.values()).filter(user => 
                        user.lastActive && user.lastActive > last24h
                    ).length;

                    notifyAdmin(
                        `📊 Hourly Statistics:\n\n` +
                        `👥 Total Users: ${totalUsers}\n` +
                        `🟢 Active (24h): ${activeUsers}\n` +
                        `⏰ Time: ${new Date().toLocaleString()}`
                    );
                } catch (error) {
                    log.error(`Failed to send hourly stats: ${error.message}`);
                }
            }, 3600000); // Every hour

            // Periodic inviter updates (her 5 dakikada bir)
            setInterval(async () => {
                try {
                    if (inviteCounter.size > 0) {
                        const batch = db.batch();

                        for (const [inviterId, count] of inviteCounter.entries()) {
                            const inviterRef = db.collection('users').doc(String(inviterId));
                            batch.update(inviterRef, {
                                bblip: admin.firestore.FieldValue.increment(1000 * count)
                            });
                            inviteCounter.delete(inviterId);
                        }

                        await batch.commit();
                        log.info(`Processed ${inviteCounter.size} pending inviter updates`);
                    }
                } catch (error) {
                    log.error(`Error processing invite updates: ${error.message}`);
                }
            }, 300000);

            // Periodic engagement system
            function startPeriodicEngagement() {
                log.system('Starting periodic engagement system...');

                // Her 6 saatte bir kontrol et
                setInterval(() => {
                    const now = Date.now();
                    const sixHours = 6 * 60 * 60 * 1000;
                    const twelveHours = 12 * 60 * 60 * 1000;
                    const oneHourInMs = 60 * 60 * 1000;

                    // Tüm kullanıcıları kontrol et
                    for (const [userId, userData] of usersMap.entries()) {
                        try {
                            // Son aktivite kontrolü
                            const lastActive = userData.lastActive || 0;
                            const timeSinceActive = now - lastActive;

                            // lastAdReward kontrolü ekle
                            const lastAdReward = userData.lastAdReward || 0;
                            const timeSinceLastReward = now - lastAdReward;

                            // Kullanıcı 12 saatten fazla aktif değilse, son mesajdan 6 saat geçtiyse
                            // VE son ödül almasından 1 saat geçtiyse
                            if (timeSinceActive > twelveHours && 
                                (!userData.lastEngagementMessage || now - userData.lastEngagementMessage >= sixHours) &&
                                timeSinceLastReward >= oneHourInMs) {

                                // Farklı zaman dilimlerine göre farklı mesajlar
                                const messages = {
                                    morning: [
                                        "🌅 Start your day with a Free Spin! Your luck is waiting!",
                                        "🌟 Morning surprise! Your Free Spin is ready to use!"
                                    ],
                                    afternoon: [
                                        "🎯 Take a break and try your luck! Free Spin available!",
                                        "✨ Perfect time for a Free Spin! Ready to win?"
                                    ],
                                    evening: [
                                        "🌙 Evening bonus: Your Free Spin is waiting for you!",
                                        "🎰 End your day with a win! Try your Free Spin now!"
                                    ]
                                };

                                // Günün saatine göre mesaj seç
                                const hour = new Date().getHours();
                                let timeMessages;
                                if (hour >= 5 && hour < 12) {
                                    timeMessages = messages.morning;
                                } else if (hour >= 12 && hour < 18) {
                                    timeMessages = messages.afternoon;
                                } else {
                                    timeMessages = messages.evening;
                                }

                                // Rastgele mesaj seç
                                const randomMessage = timeMessages[Math.floor(Math.random() * timeMessages.length)];

                                // Mesajı gönder
                                bot.sendMessage(userId, randomMessage, {
                                    reply_markup: {
                                        inline_keyboard: [[
                                            { text: "🎯 Spin Now!", web_app: { url: "https://app.bblip.io/" }}
                                        ]]
                                    }
                                }).then(() => {
                                    // Başarılı gönderimde son mesaj zamanını güncelle
                                    userData.lastEngagementMessage = now;
                                    usersMap.set(userId, userData);
                                    log.success(`Sent periodic engagement message to user ${userId}`);
                                }).catch(error => {
                                    log.error(`Failed to send periodic message to user ${userId}: ${error.message}`);
                                });
                            }
                        } catch (error) {
                            log.error(`Error in periodic engagement for user ${userId}: ${error.message}`);
                        }
                    }
                }, 30 * 60 * 1000); // Her 30 dakikada bir kontrol et

                log.success('Periodic engagement system started successfully');
            }

            // Start periodic engagement
            startPeriodicEngagement();
        });

    } catch (error) {
        log.error(`Critical error in bot: ${error.message}`);
        restartBot();
    }
}

// Restart function - modify to handle Firebase cleanup
function restartBot() {
    log.warning('Restarting bot...');

    // Clean up current process
    process.removeAllListeners();

    // Delete Firebase app instances
    Promise.all(admin.apps.map(app => app.delete())).then(() => {
        // Wait 5 seconds and restart
        setTimeout(() => {
            log.system('Starting new bot instance...');
            startBot();
        }, 5000);
    }).catch(error => {
        log.error(`Error during Firebase cleanup: ${error.message}`);
    });
}

// Global error handling
process.on('uncaughtException', (error) => {
    log.error(`Uncaught Exception: ${error.message}`);
    restartBot();
});

process.on('unhandledRejection', (reason, promise) => {
    log.error('Unhandled Rejection at:', promise);
    log.error('Reason:', reason);
    restartBot();
});

// Graceful shutdown
process.on('SIGINT', () => {
    log.warning('Received SIGINT. Shutting down...');
    process.exit(0);
});

process.on('SIGTERM', () => {
    log.warning('Received SIGTERM. Shutting down...');
    process.exit(0);
});

// Start the bot
log.system('Initial bot startup...');
startBot();
