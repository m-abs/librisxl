package whelk.plugin.libris

import spock.lang.Specification


class LibrisURIMinterSpec extends Specification {

    def "should base encode numbers"() {
        given:
        def minter = new LibrisURIMinter(alphabet: LibrisURIMinter.DEVOWELLED)
        expect:
        minter.baseEncode(n, caesared) == expected

        where:
        n               | expected      | caesared

        1               | "1"           | false
        31              | "11"          | false
        139409779957    | "6c70t5h7"    | false

        1               | "1"           | true
        30              | "10"          | true
        31              | "21"          | true
        139409779957    | "flg72dq7"    | true
    }

    def "should scramble slug"() {
        given:
        def minter = new LibrisURIMinter(
                alphabet: "0123456789bcdfghjklmnpqrstvwxz",
                slugCharInAlphabet: true,
                minSlugSize: 3)
        expect:
        minter.scramble(value) == slug
        where:
        value                   | slug
        "Märk världen"          | "mrkvrldn"
        "Det"                   | "det"
        "Där ute i mörkret"     | "drtmrkrt"
    }

    def "should shorten words in strings with many words"() {
        given:
        def minter = new LibrisURIMinter(maxWordsInSlug: 4, shortWordSize: 2)
        expect:
        minter.shorten(text) == shortened ?: text
        where:
        text                    | shortened
        "All work no play"      | null
        "All work and no play"  | "Al wo an no pl"
        "A we I m pas"          | null
        "A woe in the past"     | "A wo in th pa"
    }

    def "should compute path from data using variables and compound keys"() {
        given:
        def minter = new LibrisURIMinter(config)
        minter.metaClass.createRandom = { 898 }
        minter.metaClass.createTimestamp = { 139409779957 }
        expect:
        minter.computePath(data, "auth") == uri
        where:
        data                                        | uri
        ["@type": "Book",
            instanceTitle: [
                titleValue: "Där ute i mörkret"],
            publicationYear: "2012"]                | '/work/flg72dq7-zx-drtmrkrt2012'
        ["@type": "Record", identifier: "123"]      | '/record/123'
    }

    def config = [
        base: "//base/",
        documentUriTemplate: "{+thing}?data",
        objectLink: "about",
        alphabet: "0123456789bcdfghjklmnpqrstvwxz",
        randomVariable: "randomKey",
        maxRandom: 899,
        timestampVariable: "timeKey",
        epochDate: "2014-01-01",
        timestampCaesarCipher: true,
        slugCharInAlphabet: true,
        rulesByDataset: [
            "auth": [
                uriTemplate: "/{+basePath}/{timeKey}-{randomKey}-{compoundSlug}",
                ruleByBaseType: [
                    "CreativeWork": [
                        subclasses: ["Book"],
                        basePath: "work",
                        compoundSlugFrom: [[instanceTitle: ["titleValue"]], "publicationYear", "attributedTo"]
                    ],
                    "Record": [
                        "uriTemplate": "/record/{identifier}",
                        "variables": ["identifier"]
                    ]
                ]
            ]
        ]
    ]

}
