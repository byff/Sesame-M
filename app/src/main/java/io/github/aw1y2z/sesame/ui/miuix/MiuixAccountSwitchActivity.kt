package io.github.aw1y2z.sesame.ui.miuix

import android.content.Intent
import android.os.Bundle
import androidx.activity.compose.setContent
import androidx.compose.foundation.layout.Column
import androidx.compose.foundation.layout.Spacer
import androidx.compose.foundation.layout.fillMaxSize
import androidx.compose.foundation.layout.height
import androidx.compose.foundation.layout.padding
import androidx.compose.foundation.rememberScrollState
import androidx.compose.foundation.verticalScroll
import androidx.compose.runtime.Composable
import androidx.compose.runtime.getValue
import androidx.compose.runtime.mutableIntStateOf
import androidx.compose.runtime.mutableStateOf
import androidx.compose.runtime.remember
import androidx.compose.runtime.setValue
import androidx.compose.ui.Modifier
import androidx.compose.ui.platform.LocalContext
import androidx.compose.ui.unit.dp
import io.github.aw1y2z.sesame.data.AccountSwitchFields
import io.github.aw1y2z.sesame.util.ToastUtil
import top.yukonga.miuix.kmp.basic.Scaffold
import top.yukonga.miuix.kmp.basic.SmallTitle
import top.yukonga.miuix.kmp.preference.ArrowPreference
import top.yukonga.miuix.kmp.theme.MiuixTheme

/**
 * 多账号切换统一设置页。
 *
 * <p>这里集中管理所有与「多账号」有关的配置：自动切号开关与间隔、账号切换顺序、
 * 常态保持账号、定时切号，以及相关的滑块验证与账号数量限制。
 *
 * <p>这些配置都是全局的（值统一存在 config/config_common.json，不随当前登录账号变化），
 * 所以不挂在任何账号的独立设置里 —— 否则每个账号下都会出现一份内容相同的设置，
 * 改哪一份都会让人困惑。
 *
 * <p>入口在一级页的「设置」标签页，与「扩展功能」同级。
 */
class MiuixAccountSwitchActivity : MiuixBaseActivity() {

    /**
     * 每次回到前台都 +1，用来触发页面重算摘要。
     * <p>账号顺序页保存完是 finish() 返回的，页面状态不会自动更新，
     * 但公共配置文件里的账号列表已经变了。
     */
    val resumeTick = mutableIntStateOf(0)

    override fun onCreate(savedInstanceState: Bundle?) {
        super.onCreate(savedInstanceState)
        setAppContent {
            AccountSwitchScreen(this)
        }
    }

    override fun onResume() {
        super.onResume()
        resumeTick.intValue++
    }
}

@Composable
fun AccountSwitchScreen(activity: MiuixAccountSwitchActivity) {
    val context = LocalContext.current
    // 全局字段的值存在公共配置文件里，不是 Compose 状态：
    // 改完之后摘要不会自己刷新，用一个版本号触发重算
    var version by remember { mutableStateOf(0) }
    // 读一下这个值：回到前台时账号列表可能已变，触发重算
    activity.resumeTick.intValue

    Scaffold(
        topBar = {
            LogTopBar(
                title = "多账号切换",
                onBack = { activity.finish() }
            )
        },
        containerColor = MiuixTheme.colorScheme.surface
    ) { padding ->
        Column(
            Modifier
                .fillMaxSize()
                .verticalScroll(rememberScrollState())
                .padding(padding)
                .padding(horizontal = 16.dp, vertical = 8.dp)
        ) {
            SmallTitle(text = "切号")
            CardColumn {
                // 全局字段的改动即时写进公共配置文件，不走账号配置的落盘流程
                AccountSwitchFields.getFields().values.forEach { field ->
                    if (field.type == "ACCOUNT_LIST") {
                        // 账号顺序有专门的编辑页（上下移动 + 刷新 + 保存）
                        ArrowPreference(
                            title = field.name ?: "",
                            summary = accountOrderSummary(),
                            onClick = {
                                context.startActivity(
                                    Intent(context, MiuixAccountOrderActivity::class.java)
                                )
                            }
                        )
                    } else {
                        FieldItem(
                            field = field,
                            onGlobalChanged = {
                                version++
                                ToastUtil.show(context, "已保存")
                            },
                            refreshKey = version
                        )
                    }
                }
            }
            Spacer(Modifier.height(12.dp))
        }
    }
}
