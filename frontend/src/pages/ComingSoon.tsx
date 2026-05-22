import AppLayout from "@/components/AppLayout";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { Button } from "@/components/ui/button";
import { ExternalLink } from "lucide-react";

interface ComingSoonProps {
    title: string;
    legacyUrl: string;
    note?: string;
}

export default function ComingSoon({ title, legacyUrl, note }: ComingSoonProps) {
    return (
        <AppLayout>
            <Card className="max-w-2xl">
                <CardHeader>
                    <CardTitle>{title}</CardTitle>
                </CardHeader>
                <CardContent className="space-y-4">
                    <p className="text-sm text-muted-foreground">
                        This module is still using the legacy view while the React migration is in progress.
                        {note && <> {note}</>}
                    </p>
                    <Button asChild>
                        <a href={legacyUrl}>
                            Open legacy view <ExternalLink className="h-4 w-4" />
                        </a>
                    </Button>
                </CardContent>
            </Card>
        </AppLayout>
    );
}
