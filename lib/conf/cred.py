class WindCredential():

    def __init__(self, region):
        if region == 'nx':
            self.host = "wjx-prd-db-wind-nx-cluster.cluster-ro-cgapmdlyvedz.rds.cn-northwest-1.amazonaws.com.cn"
        elif region == 'bj':
            self.host = "wjx-production-db-wind-bj-01.ctrp3nrqkmbu.rds.cn-north-1.amazonaws.com.cn"
        elif region == 'vg':
            self.host = "wjx-production-db-wind-vg.cluster-ro-caahr2x2cdsp.us-east-1.rds.amazonaws.com"
        self.db = "wind"
        self.user = "nancyli"
        self.pwd = "ssAsPp_92233"
        self.port = 3306


class AWSCredential():

    def __init__(self):
        self.AK = "AKIA5SW3JOHGOSZB4VUO"
        self.SK   = "XoJldvWoBXwl3e6/Od+KOay7HWruIk+F/PMfxqB0"
        self.AWS_REGION = "us-east-1"
        self.SENDER  = "chinaequitiesSystem@sap-techs.com"
        #self.RECIPIENT = 'post-trade-reconcile-aaaact43ccldij6gwqbbf7kbxu@saptechs.slack.com'
        self.RECIPIENT = 'r8o6n0o6i8z5u3i0@saptechs.slack.com' #testing-for-nancy
        self.CHARSET = "UTF-8"

class SNSCred():

    def __init__(self):

        self.WebhookUrls = self.get_url_fields()
        self.channels = self.get_slack_channel()

    def ischannel(self, channel):
        assert channel in self.channels

    def get_url_fields(self):
        res = dict()
        res['testing-for-nancy'] = 'https://hooks.slack.com/services/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'
        res['log-data-futures-prod'] = 'https://hooks.slack.com/services/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'
        res['log-data-equity-prod'] = 'https://hooks.slack.com/services/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'
        res['data-systems'] = 'https://hooks.slack.com/services/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'
        res['data-systems-tickets'] = 'https://hooks.slack.com/services/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'
        return res

    def get_url(self, channel):
        self.ischannel(channel)
        webhookurls = self.get_url_fields()
        WebHookUrl = webhookurls[channel]
        return WebHookUrl

    def get_slack_channel(self):
        res = ['testing-for-nancy', 'log-data-futures-prod', 'log-data-equity-prod', 'data-systems','data-systems'
                                                                                                    '-tickets']
        return res
